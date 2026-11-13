"""Rolling monitoring uses actual persisted alerts, including existing cooldown state."""

from __future__ import annotations

import json
import time

from industrial_mlops.contracts import parse_time
from industrial_mlops.modeling import exposure_machine_days
from industrial_mlops.monitoring import drift_report, prediction_shift


def monitor_store(store, model, *, as_of=None, limit=1000):
    if not 1 <= limit <= 10000:
        raise ValueError("monitoring window exceeds the reviewed bound")
    as_of = time.time() if as_of is None else as_of
    store.db.execute("BEGIN")
    try:
        feature_rows = store.db.execute(
            "SELECT f.body,p.body prediction,l.body label,a.body alert FROM features f "
            "JOIN predictions p USING(feature_id) "
            "LEFT JOIN labels l ON l.prediction_event_id=p.event_id "
            "LEFT JOIN alerts a USING(feature_id) "
            "WHERE p.model_name=? AND p.model_version=? "
            "ORDER BY p.predicted_at DESC,f.feature_id DESC LIMIT ?",
            (model.model_name, model.model_version, limit),
        ).fetchall()
        materialized = [{key: json.loads(row[key]) if row[key] else None
                         for key in ("body", "prediction", "label", "alert")} for row in feature_rows]
        store.db.execute("COMMIT")
    except Exception:
        store.db.execute("ROLLBACK")
        raise
    materialized = [row for row in materialized if parse_time(row["body"]["available_at"]) <= as_of]
    materialized.sort(key=lambda row: (
        parse_time(row["body"]["available_at"]), parse_time(row["body"]["event_time"]),
        row["body"]["feature_id"]))
    features = [row["body"] for row in materialized]
    scored = [row for row in materialized if row["label"]
              and parse_time(row["label"]["available_at"]) <= as_of
              and parse_time(row["label"]["ingested_at"]) <= as_of]
    alerts = [row for row in scored if row["alert"]]
    true_alerts = [row for row in alerts if row["label"]["target"] == 1 and row["label"].get("failure_at")
                   and parse_time(row["label"]["failure_at"]) > parse_time(row["alert"]["available_at"])]
    false_alerts = len(alerts) - len(true_alerts)
    failures = {(row["body"]["dataset_id"], row["body"]["site_id"], row["body"]["machine_id"],
                 row["label"]["failure_at"]) for row in scored if row["label"]["target"] == 1}
    first_leads = {}
    for row in true_alerts:
        key = (row["body"]["dataset_id"], row["body"]["site_id"], row["body"]["machine_id"],
               row["label"]["failure_at"])
        lead = parse_time(row["label"]["failure_at"]) - parse_time(row["alert"]["available_at"])
        first_leads[key] = max(lead, first_leads.get(key, float("-inf")))
    exposure = exposure_machine_days([{"feature": row["body"]} for row in scored]) if scored else 0
    scores = [row["prediction"]["score"] for row in materialized]
    performance = {
        "status": "available" if scored else "awaiting_labels",
        "semantics": "actual persisted alerts; cooldown is never reconstructed at the window boundary",
        "scope": "latest scored features for the active numeric model; failures outside this window are excluded",
        "as_of": as_of, "scored_features": len(materialized), "mature_labels": len(scored),
        "pending_labels": len(materialized) - len(scored),
        "observed_alerts": len(alerts), "true_alerts": len(true_alerts), "false_alerts": false_alerts,
        "alert_precision": len(true_alerts) / len(alerts) if alerts else None,
        "machine_days": exposure,
        "exposure_estimator": "observed span plus median sampling interval for each dataset/site/machine",
        "false_alerts_per_machine_day": false_alerts / exposure if exposure else None,
        "observed_failures": len(failures), "detected_failures_in_window": len(first_leads),
        "useful_failure_recall_in_window": sum(v >= store.config.min_lead_seconds for v in first_leads.values()) / len(failures) if failures else None,
    }
    return {
        "model_version": model.model_version,
        "data_drift": drift_report(model.reference_profile, features),
        "performance": performance,
        "prediction_shift": prediction_shift(scores[:len(scores)//2], scores[len(scores)//2:],
                                             model.threshold) if len(scores) >= 20 else {
                                                 "status": "insufficient_data"},
        "promotion_performed": False,
    }
