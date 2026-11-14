"""Observable drift and delayed performance; neither signal promotes a model."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .features import FEATURE_NAMES


def _column(features: list[dict[str, Any]], name: str) -> np.ndarray:
    values = np.asarray([f["values"][name] for f in features], dtype=float)
    if not len(values) or not np.isfinite(values).all():
        raise ValueError("drift requires nonempty, finite feature values")
    return values


def reference_profile(features: list[dict[str, Any]], bins: int = 10) -> dict[str, Any]:
    if bins < 2 or bins > 50:
        raise ValueError("drift histogram must use 2..50 bins")
    result: dict[str, Any] = {"version": "1", "samples": len(features), "features": {}}
    for name in FEATURE_NAMES:
        values = _column(features, name)
        # Store finite inner edges; runtime adds open-ended tails. Constant
        # reference columns retain a narrow central bin and expose shifts.
        edges = np.unique(np.quantile(values, np.linspace(0, 1, bins + 1)[1:-1]))
        if len(edges) == 1:
            delta = max(abs(float(edges[0])) * 0.01, 0.01)
            edges = np.array([edges[0] - delta, edges[0] + delta])
        counts, _ = np.histogram(values, bins=np.concatenate(([-np.inf], edges, [np.inf])))
        result["features"][name] = {"inner_edges": edges.tolist(), "counts": counts.tolist(),
                                     "mean": float(np.mean(values)), "std": float(np.std(values))}
    return result


def drift_report(profile: dict[str, Any], features: list[dict[str, Any]], *, psi_threshold: float = 0.2,
                 min_samples: int = 100) -> dict[str, Any]:
    if not math.isfinite(psi_threshold) or psi_threshold <= 0 or min_samples < 1:
        raise ValueError("invalid drift policy")
    if profile.get("version") != "1" or set(profile.get("features", {})) != set(FEATURE_NAMES):
        raise ValueError("reference profile is incompatible with the feature contract")
    if len(features) < min_samples:
        return {"status": "insufficient_data", "samples": len(features), "min_samples": min_samples,
                "promotion_performed": False, "features": {}}
    output = {}
    for name in FEATURE_NAMES:
        reference = profile["features"][name]
        values = _column(features, name)
        edges = np.asarray(reference["inner_edges"], dtype=float)
        counts, _ = np.histogram(values, bins=np.concatenate(([-np.inf], edges, [np.inf])))
        old = np.asarray(reference["counts"], dtype=float)
        if len(old) != len(counts) or old.sum() <= 0:
            raise ValueError("invalid reference histogram")
        # Jeffreys-style smoothing keeps finite PSI for previously empty bins.
        expected = (old + 0.5) / (old.sum() + len(old) * 0.5)
        observed = (counts + 0.5) / (counts.sum() + len(counts) * 0.5)
        psi = float(np.sum((observed - expected) * np.log(observed / expected)))
        output[name] = {"psi": psi, "drifted": psi >= psi_threshold,
                        "reference_mean": reference["mean"], "current_mean": float(values.mean())}
    return {"status": "drift_detected" if any(v["drifted"] for v in output.values()) else "within_reference",
            "samples": len(features), "psi_threshold": psi_threshold, "min_samples": min_samples,
            "features": output, "promotion_performed": False}


def prediction_shift(reference_scores: list[float], current_scores: list[float], threshold: float) -> dict[str, Any]:
    reference = np.asarray(reference_scores, dtype=float)
    current = np.asarray(current_scores, dtype=float)
    if not len(reference) or not len(current) or not 0 <= threshold <= 1:
        raise ValueError("two nonempty score windows and a valid threshold are required")
    if not np.isfinite(reference).all() or not np.isfinite(current).all() or ((reference < 0) | (reference > 1)).any() or ((current < 0) | (current > 1)).any():
        raise ValueError("risk scores must be finite and in [0,1]")
    return {"reference_count": len(reference), "current_count": len(current),
            "mean_score_shift": float(current.mean() - reference.mean()),
            "reference_positive_rate": float(np.mean(reference >= threshold)),
            "current_positive_rate": float(np.mean(current >= threshold)),
            "promotion_performed": False}


def delayed_performance(features: list[dict[str, Any]], events: list[Any], predictions: dict[str, float],
                        threshold: float, config: Any, *, as_of: float) -> dict[str, Any]:
    from .modeling import evaluate_scores, labeled_rows

    scored = [f for f in features if f["feature_id"] in predictions]
    rows, coverage = labeled_rows(scored, events, as_of=as_of)
    metrics = evaluate_scores(rows, [predictions[r["feature"]["feature_id"]] for r in rows], threshold, config) if rows else None
    return {"as_of": as_of, "coverage": coverage, "metrics": metrics,
            "status": "available" if rows else "awaiting_labels", "promotion_performed": False}
