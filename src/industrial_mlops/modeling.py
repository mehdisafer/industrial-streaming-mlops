"""Causal training and maintenance-oriented model evaluation.

Only this module may choose a decision threshold, and it uses validation data.
The final temporal holdout is evaluated after that choice has been frozen.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.metadata
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .config import PipelineConfig
from .contracts import parse_time
from .features import FEATURE_NAMES, causal_feature_rows, feature_signature
from .simulator import generate_dataset


def training_source_manifest() -> dict[str, Any]:
    """Identify the actual training implementation, including an uncommitted tree."""
    package = Path(__file__).resolve().parent
    names = ("modeling.py", "registry.py", "monitoring.py", "config.py", "contracts.py", "features.py", "simulator.py")
    files = {f"src/industrial_mlops/{name}": hashlib.sha256((package / name).read_bytes()).hexdigest() for name in names}
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"files": files, "sha256": digest}


def feature_frame(features: list[dict[str, Any]]) -> pd.DataFrame:
    """Reject missing, extra or non-finite model inputs rather than impute silently."""
    rows = []
    expected = set(FEATURE_NAMES)
    for feature in features:
        values = feature["values"]
        if set(values) != expected:
            raise ValueError("feature names do not match the reviewed feature contract")
        row = []
        for name in FEATURE_NAMES:
            value = values[name]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"feature {name} must be numeric")
            if not math.isfinite(float(value)):
                raise ValueError(f"feature {name} must be finite")
            row.append(float(value))
        rows.append(row)
    return pd.DataFrame(rows, columns=list(FEATURE_NAMES), dtype=float)


class BaselineRiskModel(ClassifierMixin, BaseEstimator):
    """An explainable, untrained alarm score based on high vibration/temperature.

    A score of 0.5 corresponds to either a 5.5 mm/s vibration maximum or an
    80 degree C temperature maximum. Validation can raise the operating threshold
    to meet the maintenance budget; no coefficients are estimated from labels.
    The result is a risk score, not a calibrated failure probability.
    """

    def fit(self, X: pd.DataFrame, y: Any = None) -> BaselineRiskModel:
        self.classes_ = np.array([0, 1])
        self.n_features_in_ = len(FEATURE_NAMES)
        self.feature_names_in_ = np.asarray(FEATURE_NAMES, dtype=object)
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        frame = pd.DataFrame(X, columns=list(FEATURE_NAMES))
        vibration = (frame["vibration_max"].to_numpy() - 5.5) / 1.2
        temperature = (frame["temperature_max"].to_numpy() - 80.0) / 8.0
        z = np.clip(np.maximum(vibration, temperature), -30, 30)
        positive = 1.0 / (1.0 + np.exp(-z))
        return np.column_stack((1.0 - positive, positive))

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def labeled_rows(
    features: list[dict[str, Any]], events: list[Any], *, as_of: float
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Join an anchor to a label only after the label's availability timestamp."""
    labels: dict[str, dict[str, Any]] = {}
    for event in events:
        if not isinstance(event, dict) or event.get("kind") != "label":
            continue
        if parse_time(event["available_at"]) > as_of:
            continue
        if parse_time(event["ingested_at"]) > as_of:
            continue
        key = event["prediction_event_id"]
        prior = labels.get(key)
        if prior is not None and prior != event:
            raise ValueError("conflicting available labels for the same event")
        labels[key] = event
    result = []
    pending = 0
    for feature in features:
        if parse_time(feature["available_at"]) > as_of:
            pending += 1
            continue
        label = labels.get(feature["event_id"])
        if label is None:
            pending += 1
            continue
        for key in ("dataset_id", "site_id", "machine_id", "event_time"):
            if label[key] != feature[key]:
                raise ValueError(f"label identity mismatch: {key}")
        result.append({"feature": feature, "label": label})
    return result, {"features": len(features), "available_labels": len(result), "pending": pending}


def assert_temporal_separation(*splits: list[dict[str, Any]]) -> None:
    """Enforce disjoint data identities and label maturity before the next split."""
    used_ids: set[str] = set()
    for i, rows in enumerate(splits):
        if not rows:
            raise ValueError("a temporal split has no available labeled features")
        current_ids = {r["feature"]["dataset_id"] for r in rows}
        if used_ids & current_ids:
            raise ValueError("dataset identity is reused across temporal splits")
        used_ids.update(current_ids)
        if i:
            before = splits[i - 1]
            first = min(parse_time(r["feature"]["event_time"]) for r in rows)
            last_observation = max(parse_time(r["feature"]["event_time"]) for r in before)
            last_label = max(parse_time(r["label"]["available_at"]) for r in before)
            if first <= max(last_observation, last_label):
                raise ValueError("split overlaps earlier observations or unavailable labels")


def fit_models(rows: list[dict[str, Any]]) -> dict[str, Any]:
    X = feature_frame([r["feature"] for r in rows])
    y = np.asarray([r["label"]["target"] for r in rows], dtype=int)
    if set(y.tolist()) != {0, 1}:
        raise ValueError("training needs both healthy and pre-failure examples")
    baseline = BaselineRiskModel().fit(X, y)
    candidate = Pipeline(
        [
            ("scale", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42)),
        ]
    )
    candidate.fit(X, y)
    return {"baseline": baseline, "candidate": candidate}


def _safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def exposure_machine_days(rows: list[dict[str, Any]]) -> float:
    times: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        f = row["feature"]
        times[(f["dataset_id"], f["site_id"], f["machine_id"])].append(parse_time(f["event_time"]))
    seconds = 0.0
    for observed in times.values():
        ordered = sorted(set(observed))
        deltas = np.diff(ordered)
        step = float(np.median(deltas)) if len(deltas) else 0.0
        seconds += ordered[-1] - ordered[0] + step
    return seconds / 86400.0


def evaluate_scores(
    rows: list[dict[str, Any]], scores: Any, threshold: float, config: PipelineConfig
) -> dict[str, Any]:
    """Report event metrics and actual cooldown-limited maintenance alerts.

    A true maintenance alert must be available before the failure. Failure recall
    and lead time use the first emitted alert per failure, not repeated rows.
    """
    probabilities = np.asarray(scores, dtype=float)
    if len(rows) != len(probabilities) or not np.isfinite(probabilities).all():
        raise ValueError("one finite score is required per labeled feature")
    if ((probabilities < 0) | (probabilities > 1)).any() or not 0 <= threshold <= 1:
        raise ValueError("scores and threshold must be in [0,1]")
    y = np.asarray([r["label"]["target"] for r in rows], dtype=int)
    decision = probabilities >= threshold
    tp = int(np.sum(decision & (y == 1)))
    fp = int(np.sum(decision & (y == 0)))
    fn = int(np.sum(~decision & (y == 1)))
    failures: set[tuple[str, str, str, str]] = set()
    for row in rows:
        f, label = row["feature"], row["label"]
        if label["target"] == 1 and label.get("failure_at"):
            failures.add((f["dataset_id"], f["site_id"], f["machine_id"], label["failure_at"]))
    last_alarm: dict[tuple[str, str, str], float] = {}
    first_leads: dict[tuple[str, str, str, str], float] = {}
    alerts: list[dict[str, Any]] = []
    for index in sorted(range(len(rows)), key=lambda i: (
        parse_time(rows[i]["feature"]["available_at"]),
        parse_time(rows[i]["feature"]["event_time"]), rows[i]["feature"]["feature_id"]
    )):
        if not decision[index]:
            continue
        f, label = rows[index]["feature"], rows[index]["label"]
        key = (f["dataset_id"], f["site_id"], f["machine_id"])
        available = parse_time(f["available_at"])
        if available - last_alarm.get(key, -math.inf) < config.alert_cooldown_seconds:
            continue
        last_alarm[key] = available
        lead = parse_time(label["failure_at"]) - available if label.get("failure_at") else None
        true_alarm = bool(label["target"] == 1 and lead is not None and lead > 0)
        if true_alarm:
            first_leads.setdefault((*key, label["failure_at"]), float(lead))
        alerts.append(
            {
                "feature_id": f["feature_id"], "event_id": f["event_id"],
                "site_id": f["site_id"], "machine_id": f["machine_id"],
                "event_time": f["event_time"], "available_at": f["available_at"],
                "score": float(probabilities[index]), "target": int(label["target"]),
                "true_alarm": true_alarm, "lead_seconds": float(lead) if lead is not None else None,
                "failure_at": label.get("failure_at"),
            }
        )
    false_alerts = sum(not a["true_alarm"] for a in alerts)
    positive_alerts = len(alerts) - false_alerts
    exposure = exposure_machine_days(rows)
    lead_values = list(first_leads.values())
    useful = sum(value >= config.min_lead_seconds for value in lead_values)
    return {
        "labeled_features": len(rows), "threshold": float(threshold),
        "event_precision": _safe_ratio(tp, tp + fp), "event_recall": _safe_ratio(tp, tp + fn),
        "event_tp": tp, "event_fp": fp, "event_fn": fn,
        "alert_count": len(alerts), "true_alerts": positive_alerts, "false_alerts": false_alerts,
        "alert_precision": _safe_ratio(positive_alerts, len(alerts)),
        "machine_days": exposure,
        "false_alerts_per_machine_day": float(false_alerts / exposure) if exposure > 0 else None,
        "observed_failures": len(failures), "detected_failures": len(first_leads),
        "failure_recall": _safe_ratio(len(first_leads), len(failures)),
        "useful_failure_recall": _safe_ratio(useful, len(failures)),
        "lead_seconds_median": float(np.median(lead_values)) if lead_values else None,
        "lead_seconds_min": float(min(lead_values)) if lead_values else None,
        "alerts": alerts,
    }


def choose_threshold(rows: list[dict[str, Any]], scores: Any, config: PipelineConfig) -> tuple[float, dict[str, Any]]:
    """Choose from a fixed grid on validation only; final holdout is not an input."""
    evaluated = [evaluate_scores(rows, scores, round(float(t), 6), config) for t in np.linspace(0.05, 1.0, 39)]
    allowed = [r for r in evaluated if r["false_alerts_per_machine_day"] is not None and r["false_alerts_per_machine_day"] <= config.false_alert_budget_per_machine_day]
    if not allowed:
        raise ValueError("no threshold respects the validation false-alert budget")
    best = max(allowed, key=lambda r: (r["useful_failure_recall"], r["failure_recall"], r["alert_precision"], -r["false_alerts_per_machine_day"], r["threshold"]))
    return best["threshold"], best


def inference_latency(model: Any, features: list[dict[str, Any]], limit: int = 128) -> dict[str, Any]:
    if not features:
        raise ValueError("latency benchmark requires observations")
    frame = feature_frame(features[:limit])
    model.predict_proba(frame.iloc[:1])
    durations = []
    for i in range(len(frame)):
        start = time.perf_counter_ns()
        model.predict_proba(frame.iloc[i:i + 1])
        durations.append((time.perf_counter_ns() - start) / 1_000_000)
    return {
        "samples": len(durations), "protocol": "warm model; one row/call; model only; perf_counter_ns",
        "milliseconds": durations,
        "p50_ms": float(np.percentile(durations, 50)),
        "p95_ms": float(np.percentile(durations, 95)),
        "p99_ms": float(np.percentile(durations, 99)),
    }


def qualification(metrics: dict[str, Any], latency: dict[str, Any], config: PipelineConfig,
                  *, baseline: dict[str, Any] | None = None, label_coverage: float = 1.0) -> dict[str, Any]:
    gates = {
        "label_coverage": label_coverage >= 0.99,
        "failure_evidence": metrics["observed_failures"] >= 3,
        "exposure_evidence": metrics["machine_days"] >= 1.0,
        "false_alert_budget": metrics["false_alerts_per_machine_day"] is not None and metrics["false_alerts_per_machine_day"] <= config.false_alert_budget_per_machine_day,
        "useful_failure_recall": metrics["useful_failure_recall"] >= config.min_recall,
        "model_latency_p95": latency["p95_ms"] <= config.max_p95_inference_ms,
    }
    if baseline is not None:
        gates["improvement_over_baseline"] = bool(
            metrics["false_alerts_per_machine_day"] is not None
            and baseline["false_alerts_per_machine_day"] is not None
            and metrics["useful_failure_recall"] >= baseline["useful_failure_recall"]
            and metrics["false_alerts_per_machine_day"] <= baseline["false_alerts_per_machine_day"]
            and (
                metrics["useful_failure_recall"] > baseline["useful_failure_recall"]
                or metrics["false_alerts_per_machine_day"] < baseline["false_alerts_per_machine_day"]
                or (metrics["lead_seconds_median"] or 0) > (baseline["lead_seconds_median"] or 0)
            )
        )
    return {"eligible": all(gates.values()), "gates": gates, "policy": {
        "false_alert_budget_per_machine_day": config.false_alert_budget_per_machine_day,
        "min_recall": config.min_recall, "min_lead_seconds": config.min_lead_seconds,
        "max_p95_inference_ms": config.max_p95_inference_ms,
        "false_alerts_per_machine_day_max": config.false_alert_budget_per_machine_day,
        "useful_failure_recall_min": config.min_recall, "useful_lead_seconds_min": config.min_lead_seconds,
        "model_latency_p95_ms_max": config.max_p95_inference_ms, "label_coverage_min": 0.99,
        "observed_failures_min": 3, "machine_days_min": 1.0,
        "alert_cooldown_seconds": config.alert_cooldown_seconds,
        "candidate_improvement": "no worse useful recall and false-alert rate; strict gain in either or median actionable lead",
    }}


def train_and_register(tracking_uri: str, output_dir: str | Path, config: PipelineConfig,
                       code_sha: str, *, machines: int = 6, hours: int = 72) -> dict[str, Any]:
    """Train once, freeze validation thresholds, evaluate holdout, and register.

    This function does not move an alias or activate a worker deployment.
    """
    from .registry import RegistryClient

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    source_manifest = training_source_manifest()
    datasets: dict[str, dict[str, Any]] = {}
    rows: dict[str, list[dict[str, Any]]] = {}
    coverage: dict[str, dict[str, int]] = {}
    starts = {"train": "2026-01-01T00:00:00Z", "validation": "2026-01-05T00:00:00Z", "test": "2026-01-09T00:00:00Z"}
    if not 24 <= hours <= 72:
        raise ValueError("training profile requires 24..72 hours per non-overlapping split")
    for name, seed in (("train", 101), ("validation", 202), ("test", 303)):
        dataset = generate_dataset(seed=seed, machines=machines, sites=2, hours=hours, start=starts[name],
                                   horizon_seconds=config.horizon_seconds, lateness_seconds=config.lateness_seconds)
        datasets[name] = dataset
        features = causal_feature_rows(dataset["events"], config)
        snapshot = max(parse_time(e["ingested_at"]) for e in dataset["events"] if isinstance(e, dict))
        rows[name], coverage[name] = labeled_rows(features, dataset["events"], as_of=snapshot)
    assert_temporal_separation(rows["train"], rows["validation"], rows["test"])
    models = fit_models(rows["train"])
    report: dict[str, Any] = {
        "protocol_version": "1", "code_sha": code_sha, "feature_version": config.feature_version,
        "feature_signature": feature_signature(config),
        "training_source": source_manifest,
        "feature_names": list(FEATURE_NAMES), "split_policy": "disjoint synthetic datasets; train < validation < test; labels mature before next split",
        "starts": starts, "coverage": coverage, "datasets": {name: d["manifest"] for name, d in datasets.items()},
        "runtime": {name: importlib.metadata.version(name) for name in ("mlflow", "scikit-learn", "numpy", "pandas")},
        "models": {}, "promotion_performed": False,
    }
    holdout_scores: dict[str, np.ndarray] = {}
    for name, model in models.items():
        validation_scores = model.predict_proba(feature_frame([r["feature"] for r in rows["validation"]]))[:, 1]
        threshold, validation = choose_threshold(rows["validation"], validation_scores, config)
        # This is the first access to final-test scores; the threshold is already fixed.
        holdout_scores[name] = model.predict_proba(feature_frame([r["feature"] for r in rows["test"]]))[:, 1]
        test = evaluate_scores(rows["test"], holdout_scores[name], threshold, config)
        latency = inference_latency(model, [r["feature"] for r in rows["test"]])
        evidence = qualification(test, latency, config,
            baseline=report["models"]["baseline"]["test"] if name == "candidate" else None,
            label_coverage=_safe_ratio(coverage["test"]["available_labels"], coverage["test"]["features"]))
        report["models"][name] = {"threshold": threshold, "validation": validation, "test": test,
                                  "latency": latency, "qualification": evidence}
    report["evaluation_id"] = hashlib.sha256(json.dumps({
        "code_sha": code_sha, "datasets": report["datasets"],
        "thresholds": {n: r["threshold"] for n, r in report["models"].items()},
        "feature_names": list(FEATURE_NAMES), "config": vars(config),
    }, sort_keys=True).encode()).hexdigest()
    (destination / "evaluation-report.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    registry = RegistryClient(tracking_uri)
    if training_source_manifest() != source_manifest:
        raise RuntimeError("training source changed during evaluation; do not register uncertain lineage")
    for name, model in models.items():
        evidence = report["models"][name]
        record = registry.register(model, role=name, threshold=evidence["threshold"],
            feature_version=config.feature_version, feature_signature=feature_signature(config),
            code_sha=code_sha, evaluation_id=report["evaluation_id"],
            qualification=evidence["qualification"], metrics=evidence["test"],
            input_example=feature_frame([r["feature"] for r in rows["train"][:5]]),
            dataset_manifest=report["datasets"], reference_features=[r["feature"] for r in rows["train"]])
        evidence.update(record)
    with (destination / "holdout-predictions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["event_id", "dataset_id", "site_id", "machine_id", "event_time", "available_at", "label_available_at", "target", "failure_at", "baseline_score", "candidate_score"])
        for i, row in enumerate(rows["test"]):
            f, label = row["feature"], row["label"]
            writer.writerow([f["event_id"], f["dataset_id"], f["site_id"], f["machine_id"], f["event_time"], f["available_at"], label["available_at"], label["target"], label.get("failure_at"), float(holdout_scores["baseline"][i]), float(holdout_scores["candidate"][i])])
    (destination / "training-report.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return report
