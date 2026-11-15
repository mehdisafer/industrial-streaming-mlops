"""Bounded real Databricks Kafka -> Delta -> shared features -> MLflow job.

This qualification adapter replays a bounded Delta bronze dataset on the driver
so it can use the exact local event/feature semantics. It is not a distributed
feature engine. Each Delta merge is idempotent, but the tables are not one atomic
transaction. A completion ledger is written last and a failed batch is replayed.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import re
from statistics import median
import sys
import time
from urllib.parse import urlsplit

# Bundle source sync keeps shared code beside this file without installing a
# wheel whose local dependencies include pyspark over the vendor runtime.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from industrial_mlops.config import PipelineConfig  # noqa: E402
from industrial_mlops.contracts import ContractError, canonical_json, content_hash, parse_time, validate_event  # noqa: E402
from industrial_mlops.features import FEATURE_NAMES, feature_signature, machine_key, replay_with_report  # noqa: E402

SQL_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$")
TABLES = ("bronze", "dispositions", "accepted", "labels", "features", "predictions", "alerts", "batches", "deployment")
MAX_REPLAY_BYTES = 64 * 1024 * 1024
CHECKPOINT_BINDING = "_industrial_deployment.json"
MAX_CHECKPOINT_ENTRIES = 10000
MAX_CHECKPOINT_BINDING_BYTES = 65536
PROVIDER_PACKAGE_PINS = {
    "mlflow": "3.17.2", "numpy": "2.3.5", "pandas": "2.2.3",
    "scikit-learn": "1.8.0", "skops": "0.16.0", "scipy": "1.17.1",
    "pyarrow": "25.0.1", "databricks-sdk": "0.166.0",
}


def registry_preflight(tracking_uri: str, registry_uri: str, model_name: str) -> dict:
    """Keep the provider loader on the reviewed native UC logged-model layout."""
    if tracking_uri != "databricks" or registry_uri != "databricks-uc":
        raise ValueError(
            "Provider qualification requires tracking_uri=databricks and "
            "registry_uri=databricks-uc; workspace registries and external tracking "
            "use unsupported artifact source layouts"
        )
    parts = model_name.split(".")
    if len(parts) != 3 or not all(SQL_NAME.fullmatch(part) for part in parts):
        raise ValueError("The UC model name must contain three simple identifiers: catalog.schema.model")
    return {"tracking_uri": tracking_uri, "registry_uri": registry_uri,
            "model_name": model_name, "artifact_layout": "MLflow 3 native Databricks logged model"}


def source_batch_limits(input_rows: int, max_offsets: int, max_batch_rows: int) -> dict:
    """Separate Spark's proportional Kafka rate target from the hard sink row cap.

    Spark rounds a nonempty partition's fractional share up to one record. Its
    observed batch can therefore exceed maxOffsetsPerTrigger without violating
    the source contract. The independent row cap is enforced before collection;
    the separate total-record and byte limits bound the complete replay.
    """
    if (type(input_rows) is not int or input_rows < 0 or type(max_offsets) is not int
            or type(max_batch_rows) is not int or not 1 <= max_offsets <= max_batch_rows):
        raise ValueError("Kafka rate target must be positive and no greater than the hard sink row cap")
    if input_rows > max_batch_rows:
        raise RuntimeError("Source batch exceeded the hard sink row cap; raw records are retained and the batch is not completed")
    return {"input_rows": input_rows, "max_offsets_per_trigger": max_offsets,
            "max_batch_rows": max_batch_rows, "rate_target_exceeded": input_rows > max_offsets}


def native_dbutils_preflight(dbutils) -> dict:
    """Reject the SDK remote fallback before filesystem or table operations.

    The caller imports this object exclusively from databricks.sdk.runtime. In
    the pinned SDK that module selects either the native runtime namespace or
    RemoteDbUtils; the latter has a different FileInfo/path contract.
    """
    from databricks.sdk.dbutils import RemoteDbUtils

    if dbutils is None or isinstance(dbutils, RemoteDbUtils):
        raise RuntimeError("Native Databricks dbutils on compatible classic compute is required; the SDK RemoteDbUtils fallback is unsupported")
    fs = getattr(dbutils, "fs", None)
    if not all(callable(getattr(fs, name, None)) for name in ("ls", "head", "mkdirs", "put")):
        raise RuntimeError("The native Databricks filesystem interface is incomplete")
    return {"status": "native_interface_selected",
            "implementation": f"{type(dbutils).__module__}.{type(dbutils).__name__}",
            "scope": "SDK remote fallback excluded; filesystem operations and provider permissions still require the checkpoint guard and provider acceptance"}


def bind_checkpoint(fs, checkpoint_root: str, manifest: dict, *, namespace_exists: bool) -> dict:
    """Bind one empty checkpoint or verify its owner using documented dbutils.fs operations.

    Parent/root listings and marker reads must succeed. An error is never treated
    as proof of absence. This is a single-writer qualification precondition, not
    an object-store lock or a qualification of provider consistency semantics.
    """
    root = checkpoint_root.rstrip("/")
    parsed = urlsplit(root)
    parts = parsed.path.split("/")
    if (parsed.query or parsed.fragment or ".." in parts or "." in parts
            or (parsed.scheme and (parsed.scheme not in {"abfss", "s3", "gs"} or not parsed.netloc or not parsed.path.strip("/")))
            or (not parsed.scheme and (not root.startswith("/Volumes/") or len([part for part in parts if part]) < 5))):
        raise ValueError("Checkpoint must be a dedicated child directory in the authorized volume/cloud location, without query or traversal components")
    if manifest.get("checkpoint_root", "").rstrip("/") != root:
        raise ValueError("Checkpoint location and deployment manifest disagree")
    expected = canonical_json({"schema_version": "1", "deployment": manifest})
    if len(expected.encode("utf-8")) > MAX_CHECKPOINT_BINDING_BYTES:
        raise ValueError("Checkpoint deployment manifest exceeds its byte bound")
    parent, _, leaf = root.rpartition("/")

    def listing(path):
        # The parent must already exist and be listable. This avoids inferring
        # "missing" from an arbitrary permission/transport exception.
        entries = list(fs.ls(path))
        if len(entries) > MAX_CHECKPOINT_ENTRIES:
            raise RuntimeError("Checkpoint listing exceeds the qualification bound")
        return entries

    candidates = [entry for entry in listing(parent) if entry.name.rstrip("/") == leaf]
    if len(candidates) > 1 or (candidates and not candidates[0].name.endswith("/")):
        raise ValueError("Checkpoint path is ambiguous or is not a directory")
    exists = bool(candidates)
    if not exists and namespace_exists:
        raise ValueError("An existing Delta deployment cannot adopt a missing checkpoint")
    entries = listing(root) if exists else []
    markers = [entry for entry in entries if entry.name == CHECKPOINT_BINDING]
    other_entries = [entry for entry in entries if entry.name != CHECKPOINT_BINDING]
    marker_path = root + "/" + CHECKPOINT_BINDING

    def read_marker(entry):
        if type(entry.size) is not int or not 0 < entry.size <= MAX_CHECKPOINT_BINDING_BYTES:
            raise ValueError("Checkpoint ownership marker has an invalid size")
        value = fs.head(marker_path, MAX_CHECKPOINT_BINDING_BYTES)
        if not isinstance(value, str) or len(value.encode("utf-8")) != entry.size or value != expected:
            raise ValueError("Checkpoint belongs to a different or unreadable deployment")

    if markers:
        if len(markers) != 1:
            raise ValueError("Checkpoint ownership marker is ambiguous")
        read_marker(markers[0])
        if not namespace_exists and other_entries:
            raise ValueError("A populated checkpoint cannot initialize a new Delta namespace")
        return {"status": "matched", "marker": marker_path, "manifest_sha256": hashlib.sha256(expected.encode()).hexdigest()}
    if entries or namespace_exists:
        raise ValueError("Existing checkpoint/namespace has no matching ownership marker; preserve it and use a new isolated deployment")
    if not exists:
        if fs.mkdirs(root) is not True:
            raise RuntimeError("Could not create the dedicated checkpoint directory")
        if listing(root):
            raise RuntimeError("Checkpoint became nonempty during initialization; stop and reconcile ownership")
    if fs.put(marker_path, expected, False) is not True:
        raise RuntimeError("Checkpoint ownership marker was not created")
    verified = listing(root)
    if len(verified) != 1 or verified[0].name != CHECKPOINT_BINDING:
        raise RuntimeError("Checkpoint changed during ownership initialization; no Delta writes are allowed")
    read_marker(verified[0])
    return {"status": "created", "marker": marker_path, "manifest_sha256": hashlib.sha256(expected.encode()).hexdigest()}


def runtime_import_preflight() -> dict:
    """Check the task overlay before model access or writes; this is not provider acceptance."""
    versions = {}
    for name, expected in PROVIDER_PACKAGE_PINS.items():
        try:
            observed = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as error:
            raise RuntimeError(f"Required provider dependency is absent: {name}") from error
        if observed != expected:
            raise RuntimeError(f"Provider dependency mismatch: {name} expected {expected}, observed {observed}")
        versions[name] = observed
    version_modules = {"numpy": "numpy", "pandas": "pandas", "scipy": "scipy", "pyarrow": "pyarrow",
                       "scikit-learn": "sklearn", "mlflow": "mlflow", "skops": "skops",
                       "databricks-sdk": "databricks.sdk.version"}
    modules = {}
    loaded_versions = {}
    for package, name in version_modules.items():
        modules[name] = importlib.import_module(name)
        loaded_versions[package] = getattr(modules[name], "__version__", None)
        expected = PROVIDER_PACKAGE_PINS[package]
        if loaded_versions[package] != expected:
            raise RuntimeError(f"Loaded provider module mismatch: {package} expected {expected}, observed {loaded_versions[package]}; restart the dedicated task Python process after its library overlay")
    for name in ("scipy.linalg", "sklearn.preprocessing", "skops.io", "databricks.sdk"):
        modules[name] = importlib.import_module(name)
    np = modules["numpy"]
    values = np.asarray([1.0, 2.0])
    checks = {
        "numpy_scipy_numeric": math.isclose(float(modules["scipy.linalg"].norm(values)), math.sqrt(5.0)),
        "pyarrow_numpy_roundtrip": bool(np.array_equal(modules["pyarrow"].array(values).to_numpy(), values)),
        "pandas_numpy_roundtrip": bool(np.array_equal(modules["pandas"].DataFrame({"value": values})["value"].to_numpy(), values)),
        "sklearn_numeric": bool(np.allclose(modules["sklearn.preprocessing"].StandardScaler().fit_transform([[1.0], [3.0]]), [[-1.0], [1.0]])),
    }
    if not all(checks.values()):
        raise RuntimeError("Provider numerical import/ABI smoke failed")
    return {"status": "imports_checked", "packages": versions, "loaded_versions": loaded_versions, "checks": checks,
            "scope": "Used numerical imports and simple ABI operations only; no Spark, Kafka, Delta, registry or workspace acceptance"}


def classify_records(records: list[dict], config: PipelineConfig) -> tuple[list[dict], list[dict], list[str]]:
    """Account for every raw coordinate using the shared contract and lateness rule."""
    seen: dict[str, str] = {}
    maxima: dict[tuple, float] = {}
    assignments: dict[tuple, tuple] = {}
    accepted_sensors: dict[str, dict] = {}
    labeled_anchors: set[str] = set()
    accepted: list[dict] = []
    dispositions: list[dict] = []
    payloads: list[str] = []
    for record in records:
        raw = record["value"]
        try:
            payload = bytes(raw).decode("utf-8") if raw is not None else ""
        except UnicodeDecodeError:
            payload = "\x00"
        payloads.append(payload)
        disposition = {"topic": record["topic"], "partition": record["partition"], "offset": record["offset"],
                       "status": "rejected", "reason": None, "event_id": None}
        try:
            event = validate_event(payload, config)
            disposition["event_id"] = event["event_id"]
            digest = content_hash(event)
            if event["event_id"] in seen:
                if seen[event["event_id"]] == digest:
                    disposition.update(status="duplicate", reason="duplicate_event")
                else:
                    disposition["reason"] = "conflicting_event_id"
            else:
                seen[event["event_id"]] = digest
                key = machine_key(event)
                event_time = parse_time(event["event_time"])
                watermark = maxima.get(key, -math.inf) - config.lateness_seconds
                if event["kind"] == "sensor" and event_time <= watermark:
                    disposition["reason"] = "late_after_watermark"
                else:
                    if event["kind"] == "label":
                        target = accepted_sensors.get(event["prediction_event_id"])
                        if target is None:
                            disposition["reason"] = "label_target_not_accepted"
                        elif any(event[field] != target[field] for field in (
                            "dataset_id", "site_id", "machine_id", "event_time",
                        )):
                            disposition["reason"] = "label_target_identity_conflict"
                        elif event["prediction_event_id"] in labeled_anchors:
                            disposition["reason"] = "label_target_already_exists"
                    if disposition["reason"] is None:
                        assignment = (record["topic"], record["partition"])
                        if key in assignments and assignments[key] != assignment:
                            raise RuntimeError("A machine changed Kafka partitions; use a new topic/checkpoint for repartitioning")
                        assignments[key] = assignment
                        disposition.update(status="accepted", reason=None)
                        accepted.append(event)
                        if event["kind"] == "label":
                            labeled_anchors.add(event["prediction_event_id"])
                        else:
                            maxima[key] = max(event_time, maxima.get(key, -math.inf))
                            if event["kind"] == "sensor":
                                accepted_sensors[event["event_id"]] = event
        except ContractError as error:
            disposition["reason"] = "invalid_utf8" if payload == "\x00" else error.reason
        dispositions.append(disposition)
    return accepted, dispositions, payloads


def prepare_prediction_outputs(features: list[dict], scores: dict[str, float],
                               previous: list[dict], model_info: dict,
                               config: PipelineConfig, code_sha: str) -> tuple[list[dict], list[dict]]:
    """Keep each committed score/cooldown decision immutable and replay missing alert writes."""
    by_feature = {feature["feature_id"]: feature for feature in features}
    if len(by_feature) != len(features):
        raise ValueError("Feature identities are not unique")
    existing = {}
    last_alarm: dict[tuple, float] = {}
    for prediction in previous:
        fid = prediction["feature_id"]
        if fid in existing or fid not in by_feature:
            raise ValueError("Persisted prediction has duplicate or unknown feature identity")
        if (prediction.get("code_sha") != code_sha
                or any(prediction.get(key) != value for key, value in model_info.items())
                or any(prediction.get(key) != value for key, value in by_feature[fid].items())):
            raise ValueError("Persisted prediction differs from its immutable deployment/feature")
        expected_id = content_hash({"feature_id": fid, "model_uri": model_info["model_uri"]})
        score = prediction.get("score")
        if (prediction.get("prediction_id") != expected_id
                or isinstance(score, bool) or not isinstance(score, (int, float))
                or not math.isfinite(score) or not 0 <= score <= 1
                or type(prediction.get("alert_emitted")) is not bool
                or prediction.get("alert_policy") != "cooldown-v1"
                or prediction.get("risk_detected") != (score >= model_info["threshold"])
                or (prediction["alert_emitted"] and not prediction["risk_detected"])):
            raise ValueError("Persisted prediction lacks a valid immutable alert decision")
        existing[fid] = prediction
        if prediction["alert_emitted"]:
            key = machine_key(prediction)
            last_alarm[key] = max(parse_time(prediction["available_at"]), last_alarm.get(key, -math.inf))
    new_features = [feature for feature in features if feature["feature_id"] not in existing]
    if set(scores) != {feature["feature_id"] for feature in new_features}:
        raise ValueError("Scores must cover exactly the uncommitted features")
    new_predictions = []
    for feature in sorted(new_features, key=lambda row: (
        parse_time(row["available_at"]), parse_time(row["event_time"]), row["feature_id"],
    )):
        score = scores[feature["feature_id"]]
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Model returned a non-finite or out-of-range score")
        key = machine_key(feature)
        available = parse_time(feature["available_at"])
        detected = score >= model_info["threshold"]
        emit = detected and available - last_alarm.get(key, -math.inf) >= config.alert_cooldown_seconds
        prediction = {"prediction_id": content_hash({"feature_id": feature["feature_id"], "model_uri": model_info["model_uri"]}),
                      **feature, **model_info, "score": float(score), "risk_detected": detected,
                      "alert_emitted": emit, "alert_policy": "cooldown-v1", "code_sha": code_sha}
        new_predictions.append(prediction)
        if emit:
            last_alarm[key] = available
    # Decisions are persisted with predictions first. If a later alert/table
    # write fails, the retry recreates the same alert from that decision.
    alerts = [{"alert_id": content_hash({"prediction_id": prediction["prediction_id"], "policy": "cooldown-v1"}),
               "prediction_id": prediction["prediction_id"], "site_id": prediction["site_id"],
               "machine_id": prediction["machine_id"], "score": prediction["score"],
               "payload": canonical_json(prediction)}
              for prediction in [*previous, *new_predictions] if prediction["alert_emitted"]]
    return new_predictions, alerts


def evaluate_emitted_alerts(features: list[dict], events: list[dict], scores: dict[str, float],
                           emitted: list[dict], threshold: float, config: PipelineConfig,
                           *, as_of: float) -> dict:
    """Evaluate actual emitted rows; unavailable outcomes never reconstruct cooldown."""
    from industrial_mlops.modeling import evaluate_scores, labeled_rows

    visible = [feature for feature in features if feature["feature_id"] in scores
               and parse_time(feature["available_at"]) <= as_of]
    rows, coverage = labeled_rows(visible, events, as_of=as_of)
    metrics = evaluate_scores(rows, [scores[row["feature"]["feature_id"]] for row in rows], threshold, config) if rows else {}
    by_feature = {row["feature"]["feature_id"]: row for row in rows}
    visible_ids = {feature["feature_id"] for feature in visible}
    active_alerts = [alert for alert in emitted if alert["feature_id"] in visible_ids]
    if len({alert["feature_id"] for alert in active_alerts}) != len(active_alerts):
        raise ValueError("Multiple emitted alerts reference one feature")
    alerts = []
    first_leads = {}
    for alert in active_alerts:
        row = by_feature.get(alert["feature_id"])
        if row is None:
            continue
        feature, label = row["feature"], row["label"]
        if (alert.get("alert_emitted") is not True or alert["score"] != scores[feature["feature_id"]]
                or alert["available_at"] != feature["available_at"]):
            raise ValueError("Emitted alert does not match its persisted prediction")
        lead = parse_time(label["failure_at"]) - parse_time(alert["available_at"]) if label.get("failure_at") else None
        true_alarm = label["target"] == 1 and lead is not None and lead > 0
        if true_alarm:
            key = (*machine_key(feature), label["failure_at"])
            first_leads[key] = max(lead, first_leads.get(key, -math.inf))
        alerts.append({"alert_id": alert["alert_id"], "prediction_id": alert["prediction_id"],
                       "feature_id": feature["feature_id"], "event_id": feature["event_id"],
                       "site_id": feature["site_id"], "machine_id": feature["machine_id"],
                       "event_time": feature["event_time"], "available_at": feature["available_at"],
                       "score": alert["score"], "target": label["target"], "true_alarm": true_alarm,
                       "lead_seconds": lead, "failure_at": label.get("failure_at")})
    failures = {(*machine_key(row["feature"]), row["label"]["failure_at"])
                for row in rows if row["label"]["target"] == 1}
    true_alerts = sum(alert["true_alarm"] for alert in alerts)
    false_alerts = len(alerts) - true_alerts
    exposure = metrics.get("machine_days", 0.0)
    metrics.update({
        "status": "available" if rows else "awaiting_labels", "as_of": as_of,
        "semantics": "actual persisted alerts; cooldown is not reconstructed from labeled rows",
        "coverage": coverage, "scored_features": len(visible), "mature_labels": len(rows),
        "pending_labels": len(visible) - len(rows), "emitted_alert_count": len(active_alerts),
        "pending_alert_labels": len(active_alerts) - len(alerts),
        "labeled_features": len(rows), "threshold": threshold, "alert_count": len(alerts),
        "true_alerts": true_alerts, "false_alerts": false_alerts,
        "alert_precision": true_alerts / len(alerts) if alerts else None,
        "machine_days": exposure, "false_alerts_per_machine_day": false_alerts / exposure if exposure else None,
        "observed_failures": len(failures), "detected_failures": len(first_leads),
        "failure_recall": len(first_leads) / len(failures) if failures else None,
        "useful_failure_recall": sum(lead >= config.min_lead_seconds for lead in first_leads.values()) / len(failures) if failures else None,
        "lead_seconds_median": median(first_leads.values()) if first_leads else None,
        "lead_seconds_min": min(first_leads.values()) if first_leads else None, "alerts": alerts,
    })
    return metrics


def load_model(arguments: argparse.Namespace, config: PipelineConfig):
    registry_preflight(arguments.tracking_uri, arguments.registry_uri, arguments.model_name)
    import mlflow
    import mlflow.artifacts
    import mlflow.sklearn
    from mlflow.tracking import MlflowClient
    import yaml

    mlflow.set_tracking_uri(arguments.tracking_uri)
    mlflow.set_registry_uri(arguments.registry_uri)
    client = MlflowClient(tracking_uri=arguments.tracking_uri, registry_uri=arguments.registry_uri)
    version = client.get_model_version(arguments.model_name, arguments.model_version)
    tags = version.tags or {}
    if tags.get("publisher") != "industrial-streaming-mlops" or tags.get("feature.version") != config.feature_version:
        raise ValueError("Model publisher or feature contract does not match this project")
    if json.loads(tags.get("feature.names", "null")) != list(FEATURE_NAMES):
        raise ValueError("Model feature order does not match the shared feature contract")
    if tags.get("feature.signature") != feature_signature(config):
        raise ValueError("Model feature/window/horizon signature does not match the shared configuration")
    gates = json.loads(tags.get("qualification.gates", "{}"))
    required = {"label_coverage", "failure_evidence", "exposure_evidence", "false_alert_budget", "useful_failure_recall", "model_latency_p95"}
    if tags.get("role") == "candidate":
        required.add("improvement_over_baseline")
    if tags.get("qualification.eligible") != "true" or not tags.get("evaluation.id") or not required <= set(gates) or not all(value is True for value in gates.values()):
        raise ValueError("Model lacks the recorded held-out promotion gates")
    threshold = float(tags["decision.threshold"])
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Model threshold is invalid")
    policy = json.loads(tags.get("qualification.policy", "null"))
    expected_policy = {key: getattr(config, key) for key in (
        "alert_cooldown_seconds", "false_alert_budget_per_machine_day", "min_recall",
        "min_lead_seconds", "max_p95_inference_ms",
    )}
    if not isinstance(policy, dict) or any(policy.get(key) != value for key, value in expected_policy.items()):
        raise ValueError("Model qualification policy differs from the provider pipeline policy")
    uri = f"models:/{arguments.model_name}/{version.version}"
    # Resolve MLflow 3's logged-model indirection through the explicit client.
    # Direct models:/ artifact resolution can consult process-global settings.
    source = version.source
    if not isinstance(source, str) or not re.fullmatch(r"models:/m-[a-f0-9]+", source):
        raise ValueError("Provider model source is not the reviewed MLflow logged-model layout")
    logged = client.get_logged_model(source.split("/")[-1])
    if not version.run_id or logged.source_run_id != version.run_id:
        raise ValueError("Registered version and logged artifact have different or missing run identities")
    local = Path(mlflow.artifacts.download_artifacts(artifact_uri=logged.artifact_location, tracking_uri=arguments.tracking_uri, registry_uri=arguments.registry_uri))
    metadata = yaml.safe_load((local / "MLmodel").read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError("Model loader metadata is not an object")
    flavor = metadata.get("flavors", {}).get("sklearn", {})
    trusted_types = ["industrial_mlops.modeling.BaselineRiskModel"] if tags.get("role") == "baseline" else []
    if (tags.get("role") not in {"baseline", "candidate"} or tags.get("artifact.format") != "skops"
            or flavor.get("serialization_format") != "skops" or flavor.get("code")
            or flavor.get("pickled_model") != "model.skops"
            or (flavor.get("skops_trusted_types") or []) != trusted_types):
        raise ValueError("Model loader metadata exceeds the reviewed serializer/type policy")
    artifact = local / "model.skops"
    expected = tags.get("artifact.sha256")
    if not artifact.is_file() or hashlib.sha256(artifact.read_bytes()).hexdigest() != expected:
        raise ValueError("Model artifact does not match the recorded content digest")
    model = mlflow.sklearn.load_model(str(local))
    return model, {"model_uri": uri, "model_name": arguments.model_name, "model_version": str(version.version),
                   "model_run_id": version.run_id, "model_digest": expected, "threshold": threshold,
                   "evaluation_id": tags["evaluation.id"], "feature_version": config.feature_version,
                   "feature_signature": feature_signature(config), "artifact_format": "skops",
                   "qualification_policy": policy}


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("catalog", "schema", "table-prefix", "output-root", "checkpoint-root", "bootstrap", "topic",
                 "secret-scope", "secret-key", "model-name", "model-version", "code-sha", "run-id"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--tracking-uri", default="databricks")
    parser.add_argument("--registry-uri", default="databricks-uc")
    parser.add_argument("--max-records", type=int, default=50000)
    parser.add_argument("--max-offsets", type=int, default=1000)
    parser.add_argument("--expected-records", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=int, default=1100)
    return parser


def main() -> None:
    arguments = argument_parser().parse_args()
    for value in (arguments.catalog, arguments.schema, arguments.table_prefix):
        if not SQL_NAME.fullmatch(value):
            raise ValueError("Catalog, schema and table prefix must be simple SQL identifiers")
    if not re.fullmatch(r"[a-fA-F0-9]{40}", arguments.code_sha):
        raise ValueError("An explicit 40-character source commit is required")
    if not arguments.model_version.isdigit() or int(arguments.model_version) < 1:
        raise ValueError("An explicit numeric model version is required")
    if not arguments.run_id.isdigit():
        raise ValueError("The Databricks job run identity was not resolved")
    if not 1 <= arguments.max_offsets <= 10000 or not 1 <= arguments.max_records <= 200000:
        raise ValueError("Qualification bounds exceed the reviewed limits")
    if not 1 <= arguments.timeout_seconds <= 1100 or not 0 <= arguments.expected_records <= arguments.max_records:
        raise ValueError("Invalid timeout or expected source count")
    if not arguments.output_root.startswith("/Volumes/") or ".." in Path(arguments.output_root).parts:
        raise ValueError("Evidence output must be inside an existing authorized /Volumes location")
    if not arguments.checkpoint_root.startswith(("/Volumes/", "abfss://", "s3://", "gs://")):
        raise ValueError("A durable authorized provider checkpoint location is required")
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("This job requires a real Databricks Runtime; local Spark is not qualification")
    registry_scope = registry_preflight(arguments.tracking_uri, arguments.registry_uri, arguments.model_name)
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    runtime_imports = runtime_import_preflight()

    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession, functions as F
    from databricks.sdk.runtime import dbutils
    from industrial_mlops.modeling import feature_frame

    dbutils_preflight = native_dbutils_preflight(dbutils)
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # The reference uses ordinary rows on the driver, not Arrow/pandas UDFs.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    config = PipelineConfig.load(PROJECT_ROOT / "config" / "pipeline.json")
    source_batch_limits(0, arguments.max_offsets, config.max_batch_rows)
    model, model_info = load_model(arguments, config)
    table_names = {name: f"`{arguments.catalog}`.`{arguments.schema}`.`{arguments.table_prefix}_{name}`" for name in TABLES}

    def merge_frame(name: str, frame, schema: str, keys: tuple[str, ...]) -> None:
        table = table_names[name]
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING DELTA")
        if name == "bronze":
            existing = spark.table(table).alias("existing")
            source = frame.alias("incoming")
            joined = source.join(existing, list(keys), "inner")
            different = (~F.col("incoming.value").eqNullSafe(F.col("existing.value"))) | (~F.col("incoming.key").eqNullSafe(F.col("existing.key")))
            if joined.where(different).limit(1).count():
                raise RuntimeError("A Kafka coordinate has different raw content; stop and reconcile topic recreation or source corruption")
        condition = " AND ".join(f"target.`{key}` = incoming.`{key}`" for key in keys)
        DeltaTable.forName(spark, table).alias("target").merge(frame.alias("incoming"), condition).whenNotMatchedInsertAll().execute()

    def merge_rows(name: str, rows: list[dict], schema: str, keys: tuple[str, ...]) -> None:
        if not rows:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table_names[name]} ({schema}) USING DELTA")
            return
        merge_frame(name, spark.createDataFrame(rows, schema=schema), schema, keys)

    document_schema = "id STRING, payload STRING"
    source_paths = [Path(__file__).resolve(), PROJECT_ROOT / "config" / "pipeline.json", *sorted((PROJECT_ROOT / "src" / "industrial_mlops").glob("*.py"))]
    source_manifest = {path.relative_to(PROJECT_ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    manifest = {"code_sha": arguments.code_sha, "config": config.to_dict(), "model": model_info,
                "source_manifest": source_manifest,
                "topic": arguments.topic, "bootstrap_digest": hashlib.sha256(arguments.bootstrap.encode()).hexdigest(),
                "checkpoint_root": arguments.checkpoint_root, "catalog": arguments.catalog,
                "schema": arguments.schema, "table_prefix": arguments.table_prefix}
    deployment_exists = spark.catalog.tableExists(table_names["deployment"])
    prior = spark.table(table_names["deployment"]).where("id = 'deployment'").limit(2).collect() if deployment_exists else []
    if len(prior) > 1 or (prior and prior[0]["payload"] != canonical_json(manifest)):
        raise ValueError("This namespace belongs to a different code/config/model/source deployment; use a new isolated namespace")
    if not prior and any(spark.catalog.tableExists(table_names[name]) for name in TABLES if name != "deployment"):
        raise ValueError("Existing output tables have no deployment manifest; preserve them for reconciliation")
    checkpoint_binding = bind_checkpoint(dbutils.fs, arguments.checkpoint_root, manifest, namespace_exists=bool(prior))
    merge_rows("deployment", [{"id": "deployment", "payload": canonical_json(manifest)}], document_schema, ("id",))
    prior = spark.table(table_names["deployment"]).where("id = 'deployment'").collect()
    if len(prior) != 1 or prior[0]["payload"] != canonical_json(manifest):
        raise ValueError("This namespace belongs to a different code/config/model/source deployment; use a new isolated namespace")

    batch_reports: list[dict] = []
    latest: dict = {}
    bronze_schema = "topic STRING, partition INT, offset BIGINT, key BINARY, value BINARY, kafka_timestamp TIMESTAMP, first_batch_id BIGINT"

    def process_batch(frame, batch_id: int) -> None:
        started = time.perf_counter()
        incoming = frame.select("topic", "partition", "offset", "key", "value", F.col("timestamp").alias("kafka_timestamp")).withColumn("first_batch_id", F.lit(int(batch_id)).cast("long"))
        # Preserve binary bronze through a distributed write before any bounded
        # driver collection, including a batch containing a large toxic payload.
        merge_frame("bronze", incoming, bronze_schema, ("topic", "partition", "offset"))
        input_rows = incoming.count()
        batch_limits = source_batch_limits(input_rows, arguments.max_offsets, config.max_batch_rows)
        history_frame = spark.table(table_names["bronze"])
        footprint = history_frame.agg(F.count("*").alias("records"), F.sum(F.coalesce(F.length("value"), F.lit(0)) + F.coalesce(F.length("key"), F.lit(0))).alias("bytes")).first()
        if footprint["records"] > arguments.max_records or (footprint["bytes"] or 0) > MAX_REPLAY_BYTES:
            raise RuntimeError("Qualification dataset limit reached; raw records are retained and the batch is not completed")
        history = history_frame.orderBy("first_batch_id", "topic", "partition", "offset").collect()
        accepted, dispositions, payloads = classify_records([row.asDict() for row in history], config)
        features, replay_report = replay_with_report(payloads, config)
        counts = Counter(row["status"] for row in dispositions)
        expected = replay_report["counts"]
        if (len(history), counts["accepted"], counts["duplicate"], counts["rejected"]) != (expected["source"], expected["accepted"], expected["duplicates"], expected["rejected"]):
            raise RuntimeError("Provider disposition accounting differs from the shared reference")
        merge_rows("dispositions", dispositions, "topic STRING, partition INT, offset BIGINT, status STRING, reason STRING, event_id STRING", ("topic", "partition", "offset"))
        event_schema = "event_id STRING, dataset_id STRING, site_id STRING, machine_id STRING, kind STRING, event_time STRING, ingested_at STRING, payload STRING"
        event_rows = [{key: event[key] for key in ("event_id", "dataset_id", "site_id", "machine_id", "kind", "event_time", "ingested_at")} | {"payload": canonical_json(event)} for event in accepted]
        merge_rows("accepted", event_rows, event_schema, ("event_id",))
        merge_rows("labels", [row for row in event_rows if row["kind"] == "label"], event_schema, ("event_id",))
        feature_schema = "feature_id STRING, event_id STRING, dataset_id STRING, site_id STRING, machine_id STRING, event_time STRING, available_at STRING, feature_version STRING, feature_signature STRING, payload STRING"
        feature_rows = [{key: feature[key] for key in ("feature_id", "event_id", "dataset_id", "site_id", "machine_id", "event_time", "available_at", "feature_version", "feature_signature")} | {"payload": canonical_json(feature)} for feature in features]
        merge_rows("features", feature_rows, feature_schema, ("feature_id",))
        prior_rows = spark.table(table_names["predictions"]).limit(arguments.max_records + 1).collect() if spark.catalog.tableExists(table_names["predictions"]) else []
        if len(prior_rows) > arguments.max_records:
            raise RuntimeError("Persisted prediction count exceeds the bounded source")
        previous = [json.loads(row["payload"]) for row in prior_rows]
        previous_ids = {prediction["feature_id"] for prediction in previous}
        new_features = [feature for feature in features if feature["feature_id"] not in previous_ids]
        new_scores = {}
        if new_features:
            probabilities = model.predict_proba(feature_frame(new_features))[:, 1]
            new_scores = {feature["feature_id"]: float(score) for feature, score in zip(new_features, probabilities, strict=True)}
        new_predictions, alerts = prepare_prediction_outputs(features, new_scores, previous, model_info, config, arguments.code_sha)
        prediction_rows = [{key: prediction[key] for key in (
            "prediction_id", "feature_id", "dataset_id", "site_id", "machine_id", "score", "model_version",
        )} | {"payload": canonical_json(prediction)} for prediction in new_predictions]
        merge_rows("predictions", prediction_rows, "prediction_id STRING, feature_id STRING, dataset_id STRING, site_id STRING, machine_id STRING, score DOUBLE, model_version STRING, payload STRING", ("prediction_id",))
        merge_rows("alerts", alerts, "alert_id STRING, prediction_id STRING, site_id STRING, machine_id STRING, score DOUBLE, payload STRING", ("alert_id",))
        persisted_alert_rows = spark.table(table_names["alerts"]).limit(arguments.max_records + 1).collect()
        if len(persisted_alert_rows) > arguments.max_records:
            raise RuntimeError("Persisted alert count exceeds the bounded source")
        intended_alerts = {alert["alert_id"]: alert for alert in alerts}
        if (len(persisted_alert_rows) != len(intended_alerts)
                or any(row.asDict() != intended_alerts.get(row["alert_id"]) for row in persisted_alert_rows)):
            raise RuntimeError("Persisted alerts differ from immutable prediction decisions")
        emitted = [{**json.loads(row["payload"]), "alert_id": row["alert_id"]} for row in persisted_alert_rows]
        all_predictions = [*previous, *new_predictions]
        scores = {prediction["feature_id"]: prediction["score"] for prediction in all_predictions}
        as_of = max((parse_time(event["ingested_at"]) for event in accepted), default=0)
        evaluation = evaluate_emitted_alerts(features, accepted, scores, emitted, model_info["threshold"], config, as_of=as_of)
        report = {"batch_id": int(batch_id), "run_id": arguments.run_id, "input_rows_this_attempt": input_rows, "source_batch_limits": batch_limits, "raw_bytes": footprint["bytes"] or 0, "replay": replay_report, "label_coverage": evaluation["coverage"],
                  "model": model_info, "predictions": len(all_predictions), "new_predictions": len(new_predictions),
                  "alerts": len(emitted), "evaluation": evaluation,
                  "wall_seconds": time.perf_counter() - started, "completed_at": datetime.now(timezone.utc).isoformat()}
        # This is the final write. Partial tables from a failed batch remain
        # visible to operators but are not declared a completed batch.
        merge_rows("batches", [{"id": str(batch_id), "payload": canonical_json(report)}], document_schema, ("id",))
        batch_reports.append(report)
        latest.clear()
        latest.update(report)

    jaas = dbutils.secrets.get(scope=arguments.secret_scope, key=arguments.secret_key)
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", arguments.bootstrap).option("subscribe", arguments.topic)
              .option("startingOffsets", "earliest").option("failOnDataLoss", "true")
              .option("maxOffsetsPerTrigger", arguments.max_offsets)
              .option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.mechanism", "PLAIN")
              .option("kafka.sasl.jaas.config", jaas).load())
    query = (stream.writeStream.queryName(f"{arguments.table_prefix}_qualify")
             .option("checkpointLocation", arguments.checkpoint_root)
             .foreachBatch(process_batch).trigger(availableNow=True).start())
    try:
        if not query.awaitTermination(arguments.timeout_seconds):
            raise TimeoutError("Provider qualification exceeded its bounded duration")
    finally:
        if query.isActive:
            query.stop()
    counts = {name: spark.table(table).count() for name, table in table_names.items() if spark.catalog.tableExists(table)}
    if arguments.expected_records and counts.get("bronze", 0) != arguments.expected_records:
        raise RuntimeError("Observed raw count does not match the predeclared qualification dataset")
    if counts.get("predictions", 0) == 0:
        raise RuntimeError("No real predictions were produced; a started service is not an integration result")
    histories = {name: [row.asDict(recursive=True) for row in spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()]
                 for name, table in table_names.items() if spark.catalog.tableExists(table)}
    evidence = {"schema_version": "1", "status": "executed_pending_review", "provider": "databricks", "run_id": arguments.run_id,
                "source_manifest": source_manifest, "code_sha_origin": "deployment_parameter_to_reconcile_with_source_manifest",
                "input_rows_this_attempt": sum(report["input_rows_this_attempt"] for report in batch_reports),
                "code_sha": arguments.code_sha, "runtime": os.environ["DATABRICKS_RUNTIME_VERSION"], "spark": spark.version,
                "python": sys.version.split()[0], "packages": {name: importlib.metadata.version(name) for name in ("mlflow", "numpy", "pandas", "scikit-learn", "skops")},
                "runtime_import_preflight": runtime_imports,
                "registry_preflight": registry_scope,
                "checkpoint_binding": checkpoint_binding,
                "native_dbutils_preflight": dbutils_preflight,
                "query_id": str(query.id), "query_run_id": str(query.runId), "progress": query.recentProgress,
                "tables": counts, "table_history": histories, "latest_batch": latest, "batch_reports": batch_reports,
                "model": model_info, "consumption": None, "consumption_status": "not_observed_requires_authorized_billing_source",
                "limits": {"max_records": arguments.max_records, "max_raw_bytes": MAX_REPLAY_BYTES, "max_offsets": arguments.max_offsets, "max_batch_rows": config.max_batch_rows, "timeout_seconds": arguments.timeout_seconds,
                           "execution": "bounded driver replay with idempotent per-table Delta merges; not distributed feature scalability"}}
    output = Path(arguments.output_root) / arguments.table_prefix / f"run-{arguments.run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n")
    print(json.dumps({"provider_run_id": arguments.run_id, "evidence_path": str(output), "tables": counts, "status": evidence["status"]}))


if __name__ == "__main__":
    main()
