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
import importlib.metadata
import json
import math
import os
from pathlib import Path
import re
import sys
import time

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


def classify_records(records: list[dict], config: PipelineConfig) -> tuple[list[dict], list[dict], list[str]]:
    """Account for every raw coordinate using the shared contract and lateness rule."""
    seen: dict[str, str] = {}
    maxima: dict[tuple, float] = {}
    assignments: dict[tuple, tuple] = {}
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
                assignment = (record["topic"], record["partition"])
                if key in assignments and assignments[key] != assignment:
                    raise RuntimeError("A machine changed Kafka partitions; use a new topic/checkpoint for repartitioning")
                assignments[key] = assignment
                event_time = parse_time(event["event_time"])
                watermark = maxima.get(key, -math.inf) - config.lateness_seconds
                if event["kind"] == "sensor" and event_time <= watermark:
                    disposition["reason"] = "late_after_watermark"
                else:
                    disposition.update(status="accepted", reason=None)
                    accepted.append(event)
                    if event["kind"] != "label":
                        maxima[key] = max(event_time, maxima.get(key, -math.inf))
        except ContractError as error:
            disposition["reason"] = "invalid_utf8" if payload == "\x00" else error.reason
        dispositions.append(disposition)
    return accepted, dispositions, payloads


def load_model(arguments: argparse.Namespace, config: PipelineConfig):
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
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession, functions as F
    from databricks.sdk.runtime import dbutils
    from industrial_mlops.modeling import evaluate_scores, feature_frame, labeled_rows

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # The reference uses ordinary rows on the driver, not Arrow/pandas UDFs.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    config = PipelineConfig.load(PROJECT_ROOT / "config" / "pipeline.json")
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
                "checkpoint_root": arguments.checkpoint_root, "table_prefix": arguments.table_prefix}
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
        if input_rows > arguments.max_offsets:
            raise RuntimeError("Source batch exceeded the configured bound")
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
        predictions = []
        alerts = []
        scores: dict[str, float] = {}
        if features:
            probabilities = model.predict_proba(feature_frame(features))[:, 1]
            scores = {feature["feature_id"]: float(score) for feature, score in zip(features, probabilities, strict=True)}
            if any(not math.isfinite(score) or not 0 <= score <= 1 for score in scores.values()):
                raise ValueError("Model returned a non-finite or out-of-range score")
            last_alarm: dict[tuple, float] = {}
            for feature in sorted(features, key=lambda row: (parse_time(row["available_at"]), row["feature_id"])):
                score = scores[feature["feature_id"]]
                identity = {"feature_id": feature["feature_id"], "model_uri": model_info["model_uri"]}
                prediction = {"prediction_id": content_hash(identity), **feature, **model_info,
                              "score": score, "risk_detected": score >= model_info["threshold"], "code_sha": arguments.code_sha}
                predictions.append({"prediction_id": prediction["prediction_id"], "feature_id": feature["feature_id"],
                                    "dataset_id": feature["dataset_id"], "site_id": feature["site_id"], "machine_id": feature["machine_id"],
                                    "score": score, "model_version": model_info["model_version"], "payload": canonical_json(prediction)})
                key = machine_key(feature)
                available = parse_time(feature["available_at"])
                if prediction["risk_detected"] and available - last_alarm.get(key, -math.inf) >= config.alert_cooldown_seconds:
                    last_alarm[key] = available
                    alerts.append({"alert_id": content_hash({"prediction_id": prediction["prediction_id"], "policy": "cooldown-v1"}),
                                   "prediction_id": prediction["prediction_id"], "site_id": feature["site_id"], "machine_id": feature["machine_id"],
                                   "score": score, "payload": canonical_json(prediction)})
        merge_rows("predictions", predictions, "prediction_id STRING, feature_id STRING, dataset_id STRING, site_id STRING, machine_id STRING, score DOUBLE, model_version STRING, payload STRING", ("prediction_id",))
        merge_rows("alerts", alerts, "alert_id STRING, prediction_id STRING, site_id STRING, machine_id STRING, score DOUBLE, payload STRING", ("alert_id",))
        as_of = max((parse_time(event["ingested_at"]) for event in accepted), default=0)
        joined, coverage = labeled_rows(features, accepted, as_of=as_of)
        evaluation = evaluate_scores(joined, [scores[row["feature"]["feature_id"]] for row in joined], model_info["threshold"], config) if joined else None
        report = {"batch_id": int(batch_id), "run_id": arguments.run_id, "input_rows_this_attempt": input_rows, "raw_bytes": footprint["bytes"] or 0, "replay": replay_report, "label_coverage": coverage,
                  "model": model_info, "predictions": len(predictions), "alerts": len(alerts), "evaluation": evaluation,
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
                "query_id": str(query.id), "query_run_id": str(query.runId), "progress": query.recentProgress,
                "tables": counts, "table_history": histories, "latest_batch": latest, "batch_reports": batch_reports,
                "model": model_info, "consumption": None, "consumption_status": "not_observed_requires_authorized_billing_source",
                "limits": {"max_records": arguments.max_records, "max_raw_bytes": MAX_REPLAY_BYTES, "max_offsets": arguments.max_offsets, "timeout_seconds": arguments.timeout_seconds,
                           "execution": "bounded driver replay with idempotent per-table Delta merges; not distributed feature scalability"}}
    output = Path(arguments.output_root) / arguments.table_prefix / f"run-{arguments.run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n")
    print(json.dumps({"provider_run_id": arguments.run_id, "evidence_path": str(output), "tables": counts, "status": evidence["status"]}))


if __name__ == "__main__":
    main()
