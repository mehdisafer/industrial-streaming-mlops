"""Single-writer SQLite reference sink with atomic Kafka-coordinate reconciliation.

Spark is at-least-once at this boundary. Raw dispositions, features, predictions,
alerts and the batch ledger commit together. Replays cannot duplicate effects.
This is deliberately a bounded local sink, not a distributed storage claim.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import ContractError, parse_time
from industrial_mlops.features import build_features, feature_signature


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def source_bytes(record: dict) -> bytes:
    if record.get("value_base64") is not None:
        # Spark's base64 expression can insert MIME CR/LF wrapping at 76 columns.
        encoded = record["value_base64"].replace("\r", "").replace("\n", "")
        return base64.b64decode(encoded, validate=True)
    return record["value"].encode("utf-8", errors="surrogatepass") if record["value"] is not None else b""


def tombstone(record: dict) -> bool:
    return bool(record.get("value_is_null") or (
        record.get("value") is None and record.get("value_base64") is None))


SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS raw_records(
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
 payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL, published_at REAL,
 received_at REAL NOT NULL, event_id TEXT, disposition TEXT NOT NULL, reason TEXT,
 payload_base64 TEXT NOT NULL,
 payload_is_null INTEGER NOT NULL,
 PRIMARY KEY(topic,partition_id,offset_id));
CREATE TABLE IF NOT EXISTS seen_events(event_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(
 event_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, site_id TEXT NOT NULL,
 machine_id TEXT NOT NULL, event_time REAL NOT NULL, ingested_at REAL NOT NULL,
 body TEXT NOT NULL, finalized INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS events_window
 ON events(dataset_id,site_id,machine_id,event_time);
CREATE INDEX IF NOT EXISTS events_pending ON events(finalized,event_time);
CREATE TABLE IF NOT EXISTS labels(
 event_id TEXT PRIMARY KEY, prediction_event_id TEXT UNIQUE NOT NULL,
 available_at REAL NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS watermarks(
 dataset_id TEXT NOT NULL, site_id TEXT NOT NULL, machine_id TEXT NOT NULL,
 max_event_time REAL NOT NULL, PRIMARY KEY(dataset_id,site_id,machine_id));
CREATE TABLE IF NOT EXISTS features(
 feature_id TEXT PRIMARY KEY, event_id TEXT UNIQUE NOT NULL, body TEXT NOT NULL,
 created_at REAL NOT NULL, last_error TEXT);
CREATE TABLE IF NOT EXISTS predictions(
 feature_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
 site_id TEXT NOT NULL, machine_id TEXT NOT NULL, event_time REAL NOT NULL,
 available_at REAL NOT NULL, predicted_at REAL NOT NULL, score REAL NOT NULL,
 threshold REAL NOT NULL, model_name TEXT NOT NULL, model_version TEXT NOT NULL,
 run_id TEXT NOT NULL, feature_version TEXT NOT NULL, mode TEXT NOT NULL,
 inference_ms REAL NOT NULL, arrival_to_prediction_ms REAL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS alerts(
 alert_id TEXT PRIMARY KEY, feature_id TEXT UNIQUE NOT NULL, dataset_id TEXT NOT NULL,
 site_id TEXT NOT NULL, machine_id TEXT NOT NULL, event_time REAL NOT NULL,
 available_at REAL NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches(
 query_id TEXT NOT NULL, batch_id INTEGER NOT NULL, input_sha256 TEXT NOT NULL,
 rows_received INTEGER NOT NULL, committed_at REAL NOT NULL, summary TEXT NOT NULL,
 PRIMARY KEY(query_id,batch_id));
CREATE TABLE IF NOT EXISTS deployments(
 id INTEGER PRIMARY KEY AUTOINCREMENT, model_name TEXT NOT NULL,
 model_version TEXT NOT NULL, run_id TEXT NOT NULL, feature_version TEXT NOT NULL,
 threshold REAL NOT NULL, mode TEXT NOT NULL, action TEXT NOT NULL, activated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS runtime(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at REAL NOT NULL);
"""


class InjectedFailure(RuntimeError):
    """A controlled failure used to test the transactional/checkpoint boundary."""


class Store:
    def __init__(
        self, path: str | Path, config: PipelineConfig | None = None, *, readonly: bool = False,
    ):
        self.path = Path(path)
        self.config = config or PipelineConfig()
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(
            self.path.resolve().as_uri() + "?mode=ro" if readonly else self.path,
            timeout=15, isolation_level=None, uri=readonly,
        )
        self.db.row_factory = sqlite3.Row
        if readonly:
            for key, value in (
                ("schema_version", "1"), ("config_sha256", digest(asdict(self.config))),
            ):
                row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
                if row is None or row["value"] != value:
                    self.db.close()
                    raise ValueError(f"incompatible persisted {key}")
            return
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=15000")
        self.db.executescript(SCHEMA)
        self._set_metadata("schema_version", "1")
        self._set_metadata("config_sha256", digest(asdict(self.config)))
        self._set_metadata("config", canonical(asdict(self.config)))

    def _set_metadata(self, key: str, value: str) -> None:
        current = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if current and current["value"] != value:
            raise ValueError(f"incompatible persisted {key}; migrate to a new store explicitly")
        self.db.execute("INSERT OR IGNORE INTO metadata VALUES (?,?)", (key, value))

    def close(self) -> None:
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def set_runtime(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO runtime VALUES (?,?,?) ON CONFLICT(key) DO UPDATE "
            "SET value=excluded.value,updated_at=excluded.updated_at",
            (key, canonical(value), time.time()),
        )

    def deployment(self) -> dict | None:
        row = self.db.execute("SELECT * FROM deployments ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def activate(self, model: Any, *, shadow: bool = False, action: str = "activate") -> dict:
        if model.feature_version != self.config.feature_version:
            raise ValueError("model feature contract is incompatible with the pipeline")
        if model.feature_signature != feature_signature(self.config):
            raise ValueError("model feature semantics are incompatible with the pipeline")
        if not shadow:
            policy = getattr(model, "qualification_policy", {})
            expected = {
                key: getattr(self.config, key) for key in (
                    "alert_cooldown_seconds", "false_alert_budget_per_machine_day",
                    "min_recall", "min_lead_seconds", "max_p95_inference_ms",
                )
            }
            if any(policy.get(key) != value for key, value in expected.items()):
                raise ValueError("model qualification policy differs from pipeline policy")
        if not shadow and not getattr(model, "qualified", False):
            raise ValueError("model is not qualified; explicit shadow mode is required")
        self.db.execute(
            "INSERT INTO deployments(model_name,model_version,run_id,feature_version,"
            "threshold,mode,action,activated_at) VALUES (?,?,?,?,?,?,?,?)",
            (model.model_name, str(model.model_version), model.run_id, model.feature_version,
             model.threshold, "shadow" if shadow else "approved", action, time.time()),
        )
        return self.deployment()

    def previous_deployment(self) -> dict:
        rows = self.db.execute("SELECT * FROM deployments ORDER BY id DESC LIMIT 2").fetchall()
        if len(rows) < 2:
            raise ValueError("no previous compatible deployment exists")
        return dict(rows[1])

    def process_batch(
        self, records: list[dict], query_id: str, batch_id: int, model: Any = None,
        *, failpoint: str | None = None,
    ) -> dict:
        """Process complete bounded input. A repeated batch must have identical content."""
        from industrial_mlops.contracts import validate_event

        if len(records) > self.config.max_batch_rows:
            raise ValueError("micro-batch exceeds configured bound")
        if sum(len(source_bytes(record)) for record in records) > self.config.max_batch_bytes:
            raise ValueError("micro-batch exceeds configured byte bound")
        records = sorted(records, key=lambda r: (r["topic"], r["partition"], r["offset"]))
        input_hash = digest([
            [r["topic"], r["partition"], r["offset"], hashlib.sha256(source_bytes(r)).hexdigest(), tombstone(r)]
            for r in records
        ])
        self.db.execute("BEGIN IMMEDIATE")
        summary = {"received": len(records), "new_coordinates": 0, "replayed_coordinates": 0}
        try:
            previous = self.db.execute(
                "SELECT * FROM batches WHERE query_id=? AND batch_id=?", (query_id, batch_id)
            ).fetchone()
            if previous:
                if previous["input_sha256"] != input_hash:
                    raise ValueError("batch identity reused with different input; preserve checkpoint")
                self.db.execute("COMMIT")
                return {**json.loads(previous["summary"]), "batch_replay": True}
            for record in records:
                coordinate = (record["topic"], record["partition"], record["offset"])
                original = source_bytes(record)
                invalid_utf8 = False
                try:
                    payload = original.decode("utf-8", errors="strict")
                except UnicodeDecodeError:
                    payload, invalid_utf8 = "", True
                payload_sha = hashlib.sha256(original).hexdigest()
                existing = self.db.execute(
                    "SELECT payload_sha256,payload_is_null FROM raw_records WHERE topic=? AND partition_id=? "
                    "AND offset_id=?", coordinate,
                ).fetchone()
                if existing:
                    if (existing["payload_sha256"] != payload_sha or
                            bool(existing["payload_is_null"]) != tombstone(record)):
                        raise ValueError("Kafka coordinate content changed; topic recreation is unsafe")
                    summary["replayed_coordinates"] += 1
                    continue
                event_id = None
                disposition, reason = "rejected", None
                try:
                    if tombstone(record):
                        raise ContractError("kafka_tombstone", "Null Kafka payload cannot be a sensor event")
                    if invalid_utf8:
                        raise ContractError("invalid_utf8", "Kafka payload is not UTF-8 text")
                    event = validate_event(payload, self.config)
                except ContractError as exc:
                    reason = f"contract:{str(exc)[:160]}"
                else:
                    event_id = event["event_id"]
                    event_hash = digest(event)
                    seen = self.db.execute(
                        "SELECT payload_sha256 FROM seen_events WHERE event_id=?", (event_id,)
                    ).fetchone()
                    if seen:
                        if seen["payload_sha256"] == event_hash:
                            disposition, reason = "duplicate", "event_id_already_seen"
                        else:
                            disposition, reason = "rejected", "event_id_content_conflict"
                    else:
                        self.db.execute("INSERT INTO seen_events VALUES (?,?)", (event_id, event_hash))
                        disposition, reason = self._accept(event)
                # Retain poison payloads too; source size is bounded before delivery.
                self.db.execute(
                    "INSERT INTO raw_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (*coordinate, payload, payload_sha, record.get("published_at"),
                     time.time(), event_id, disposition, reason,
                     base64.b64encode(original).decode("ascii"), int(tombstone(record))),
                )
                summary["new_coordinates"] += 1
            summary["predictions_written"] = self._score_pending(model)
            summary["dispositions"] = self.counts()
            self.db.execute(
                "INSERT INTO batches VALUES (?,?,?,?,?,?)",
                (query_id, batch_id, input_hash, len(records), time.time(), canonical(summary)),
            )
            self.set_runtime("last_batch", {"query_id": query_id, "batch_id": batch_id, **summary})
            if failpoint == "before_commit":
                raise InjectedFailure("before_commit")
            self.db.execute("COMMIT")
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise
        if failpoint == "after_commit":
            raise InjectedFailure("after_commit")
        return summary

    def _accept(self, event: dict) -> tuple[str, str | None]:
        kind = event["kind"]
        if kind == "label":
            target = self.db.execute(
                "SELECT body FROM events WHERE event_id=?", (event["prediction_event_id"],)
            ).fetchone()
            if target is None:
                return "rejected", "label_target_not_accepted"
            target_event = json.loads(target["body"])
            if any(event[field] != target_event[field] for field in (
                "dataset_id", "site_id", "machine_id", "event_time",
            )):
                return "rejected", "label_target_identity_conflict"
            previous = self.db.execute(
                "SELECT body FROM labels WHERE prediction_event_id=?",
                (event["prediction_event_id"],),
            ).fetchone()
            if previous:
                return "rejected", "label_target_already_exists"
            self.db.execute(
                "INSERT INTO labels VALUES (?,?,?,?)",
                (event["event_id"], event["prediction_event_id"], parse_time(event["available_at"]),
                 canonical(event)),
            )
            return "label", None
        key = (event["dataset_id"], event["site_id"], event["machine_id"])
        event_time = parse_time(event["event_time"])
        current = self.db.execute(
            "SELECT max_event_time FROM watermarks WHERE dataset_id=? AND site_id=? AND machine_id=?",
            key,
        ).fetchone()
        max_seen = current["max_event_time"] if current else float("-inf")
        if kind == "sensor" and event_time <= max_seen - self.config.lateness_seconds:
            return "rejected", "beyond_lateness"
        if kind == "sensor":
            self.db.execute(
                "INSERT INTO events(event_id,dataset_id,site_id,machine_id,event_time,ingested_at,body) "
                "VALUES (?,?,?,?,?,?,?)",
                (event["event_id"], *key, event_time, parse_time(event["ingested_at"]), canonical(event)),
            )
        new_max = max(max_seen, event_time)
        self.db.execute(
            "INSERT INTO watermarks VALUES (?,?,?,?) ON CONFLICT(dataset_id,site_id,machine_id) "
            "DO UPDATE SET max_event_time=excluded.max_event_time", (*key, new_max),
        )
        watermark = new_max - self.config.lateness_seconds
        pending = self.db.execute(
            "SELECT event_id,event_time,body FROM events WHERE dataset_id=? AND site_id=? "
            "AND machine_id=? AND finalized=0 AND event_time<=? ORDER BY event_time,event_id",
            (*key, watermark),
        ).fetchall()
        for anchor in pending:
            history = self.db.execute(
                "SELECT body FROM events WHERE dataset_id=? AND site_id=? AND machine_id=? "
                "AND event_time>? AND event_time<=? ORDER BY event_time,event_id",
                (*key, anchor["event_time"] - self.config.window_seconds, anchor["event_time"]),
            ).fetchall()
            feature = build_features(
                [json.loads(row["body"]) for row in history], json.loads(anchor["body"]), self.config,
                as_of=parse_time(event["ingested_at"]),
            )
            if feature:
                self.db.execute(
                    "INSERT INTO features(feature_id,event_id,body,created_at) VALUES (?,?,?,?)",
                    (feature["feature_id"], feature["event_id"], canonical(feature), time.time()),
                )
            self.db.execute("UPDATE events SET finalized=1 WHERE event_id=?", (anchor["event_id"],))
        return ("accepted", None) if kind == "sensor" else ("control", None)

    def _score_pending(self, model: Any) -> int:
        if model is None:
            self.set_runtime("model", {"status": "unavailable", "pending_retained": True})
            return 0
        if (model.feature_version != self.config.feature_version or
                model.feature_signature != feature_signature(self.config)):
            self.set_runtime("model", {"status": "incompatible", "pending_retained": True})
            return 0
        deployment = self.deployment()
        mode = deployment["mode"] if deployment else "shadow"
        if deployment and any(str(deployment[field]) != str(getattr(model, field)) for field in (
            "model_name", "model_version", "run_id", "feature_version", "threshold",
        )):
            raise ValueError("loaded model differs from activated model identity")
        rows = self.db.execute(
            "SELECT f.* FROM features f LEFT JOIN predictions p USING(feature_id) "
            "WHERE p.feature_id IS NULL ORDER BY f.created_at,f.feature_id LIMIT ?",
            (self.config.max_batch_rows,),
        ).fetchall()
        # Feature time controls cooldown, not insertion order or randomized identity.
        rows = sorted(rows, key=lambda row: (
            parse_time(json.loads(row["body"])["available_at"]),
            parse_time(json.loads(row["body"])["event_time"]),
            row["feature_id"],
        ))
        written, failures = 0, 0
        for row in rows:
            feature = json.loads(row["body"])
            started = time.perf_counter()
            try:
                score = float(model.predict(feature))
                if not math.isfinite(score) or not 0 <= score <= 1:
                    raise ValueError("model probability is not finite in [0,1]")
            except Exception as exc:
                failures += 1
                self.db.execute(
                    "UPDATE features SET last_error=? WHERE feature_id=?",
                    (type(exc).__name__, feature["feature_id"]),
                )
                continue
            latency = (time.perf_counter() - started) * 1000
            predicted_at = time.time()
            published = self.db.execute(
                "SELECT MIN(published_at) AS published FROM raw_records WHERE event_id=?",
                (feature["event_id"],),
            ).fetchone()["published"]
            e2e = max(0.0, (predicted_at - published) * 1000) if published else None
            prediction = {
                "schema_version": "1", "feature_id": feature["feature_id"],
                "event_id": feature["event_id"], "dataset_id": feature["dataset_id"],
                "site_id": feature["site_id"], "machine_id": feature["machine_id"],
                "event_time": feature["event_time"], "available_at": feature["available_at"],
                "score": score, "threshold": model.threshold, "model_name": model.model_name,
                "model_version": str(model.model_version), "run_id": model.run_id,
                "feature_version": model.feature_version,
                "feature_signature": model.feature_signature, "mode": mode,
                "predicted_at_epoch": predicted_at, "inference_ms": latency,
                "arrival_to_prediction_ms": e2e, "source_event_ids": feature["source_event_ids"],
            }
            self.db.execute(
                "INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (feature["feature_id"], feature["event_id"], feature["dataset_id"], feature["site_id"],
                 feature["machine_id"], parse_time(feature["event_time"]),
                 parse_time(feature["available_at"]), predicted_at, score, model.threshold,
                 model.model_name, str(model.model_version), model.run_id, model.feature_version,
                 mode, latency, e2e, canonical(prediction)),
            )
            self.db.execute("UPDATE features SET last_error=NULL WHERE feature_id=?", (row["feature_id"],))
            if score >= model.threshold:
                self._alert(feature, prediction)
            written += 1
        self.set_runtime("model", {
            "status": "degraded" if failures else "ready", "model_version": str(model.model_version),
            "failures": failures, "mode": mode,
        })
        return written

    def _alert(self, feature: dict, prediction: dict) -> None:
        key = (feature["dataset_id"], feature["site_id"], feature["machine_id"])
        available = parse_time(feature["available_at"])
        previous = self.db.execute(
            "SELECT MAX(available_at) AS last FROM alerts WHERE dataset_id=? AND site_id=? "
            "AND machine_id=?", key,
        ).fetchone()["last"]
        if previous is not None and available - previous < self.config.alert_cooldown_seconds:
            return
        alert_id = digest(["alert", feature["feature_id"], prediction["model_version"]])
        body = {
            "schema_version": "1", "alert_id": alert_id, **prediction,
            "observations": feature["values"],
            "reason": "Risk exceeds the locked model threshold; inspect the linked sensor window.",
            "evidence": {"feature_id": feature["feature_id"], "source_event_ids": feature["source_event_ids"]},
        }
        self.db.execute(
            "INSERT INTO alerts VALUES (?,?,?,?,?,?,?,?)",
            (alert_id, feature["feature_id"], *key, parse_time(feature["event_time"]), available,
             canonical(body)),
        )

    def score_pending(self, model: Any) -> int:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            count = self._score_pending(model)
            self.db.execute("COMMIT")
            return count
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def counts(self) -> dict:
        result = {key: 0 for key in ("accepted", "rejected", "duplicate", "label", "control")}
        result.update({
            row["disposition"]: row["n"]
            for row in self.db.execute("SELECT disposition,COUNT(*) AS n FROM raw_records GROUP BY disposition")
        })
        result["raw"] = sum(result.values())
        for table in ("events", "labels", "features", "predictions", "alerts", "batches"):
            result[table] = self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        result["pending_predictions"] = result["features"] - result["predictions"]
        result["pending_events"] = self.db.execute(
            "SELECT COUNT(*) FROM events WHERE finalized=0"
        ).fetchone()[0]
        return result

    def verify(self) -> dict:
        counts = self.counts()
        checks = {
            "source_dispositions_balance": counts["raw"] == sum(counts[key] for key in (
                "accepted", "rejected", "duplicate", "label", "control")),
            "accepted_events_balance": counts["accepted"] == counts["events"],
            "labels_balance": counts["label"] == counts["labels"],
            "predictions_have_features": self.db.execute(
                "SELECT COUNT(*) FROM predictions p LEFT JOIN features f USING(feature_id) "
                "WHERE f.feature_id IS NULL").fetchone()[0] == 0,
            "alerts_have_predictions": self.db.execute(
                "SELECT COUNT(*) FROM alerts a LEFT JOIN predictions p USING(feature_id) "
                "WHERE p.feature_id IS NULL").fetchone()[0] == 0,
            "sqlite_integrity": self.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok",
        }
        return {"checks": checks, "valid": all(checks.values()), "counts": counts}

    def snapshot(self, *, as_of: float | None = None) -> dict:
        as_of = time.time() if as_of is None else as_of
        rows = self.db.execute(
            "SELECT p.body,l.body AS label FROM predictions p LEFT JOIN labels l "
            "ON p.event_id=l.prediction_event_id AND l.available_at<=? "
            "ORDER BY p.predicted_at DESC LIMIT 200", (as_of,),
        ).fetchall()
        predictions = []
        for row in rows:
            label = json.loads(row["label"]) if row["label"] else None
            if label and parse_time(label["ingested_at"]) > as_of:
                label = None
            predictions.append({**json.loads(row["body"]), "label": label})
        runtime = {
            row["key"]: {"value": json.loads(row["value"]), "updated_at": row["updated_at"]}
            for row in self.db.execute("SELECT * FROM runtime")
        }
        last_batch = runtime.get("last_batch", {}).get("updated_at")
        return {
            "generated_at": time.time(), "as_of": as_of, "counts": self.counts(),
            "deployment": self.deployment(), "runtime": runtime,
            "freshness_seconds": time.time() - last_batch if last_batch else None,
            "predictions": predictions,
            "alerts": [json.loads(row["body"]) for row in self.db.execute(
                "SELECT body FROM alerts ORDER BY available_at DESC LIMIT 100")],
            "rejections": [dict(row) for row in self.db.execute(
                "SELECT reason,COUNT(*) AS count FROM raw_records WHERE disposition='rejected' "
                "GROUP BY reason ORDER BY count DESC")],
        }
