"""Rolling monitoring regressions using SQLite and an explicit unit predictor.

These tests do not claim Kafka, MLflow or provider integration qualification.
"""

import copy
import json

from industrial_mlops.contracts import canonical_json, iso_time, parse_time
from industrial_mlops.features import causal_feature_rows, feature_signature
from industrial_mlops.modeling import evaluate_scores
from industrial_mlops.monitoring import reference_profile
from industrial_mlops.observability import monitor_store
from industrial_mlops.simulator import generate_dataset
from industrial_mlops.store import Store


class UnitPredictor:
    model_name = "observability-unit-test-only"
    model_version = "1"
    run_id = "observability-unit-run"
    feature_version = "v1"
    feature_signature = feature_signature()
    threshold = 0.6
    qualified = True
    qualification_policy = {
        "alert_cooldown_seconds": 1800,
        "false_alert_budget_per_machine_day": 2.0,
        "min_recall": 0.70,
        "min_lead_seconds": 300,
        "max_p95_inference_ms": 1000.0,
    }

    def __init__(self, reference_features):
        self.reference_profile = reference_profile(reference_features)

    def predict(self, feature):
        return 0.9


def fill_store(store, dataset, model):
    store.activate(model)
    records = [
        {"topic": "monitoring-unit", "partition": 0, "offset": index,
         "value": canonical_json(event)}
        for index, event in enumerate(dataset["events"])
    ]
    store.process_batch(records, "monitoring-unit", 0, model)


def transport_delayed_label_fixture():
    """Delay one label's delivery after every outcome has already matured."""
    dataset = generate_dataset(machines=1, sites=1, hours=1)
    features = causal_feature_rows(dataset["events"])
    first_feature = min(features, key=lambda feature: parse_time(feature["available_at"]))
    historical_cutoff = max(parse_time(event["ingested_at"]) for event in dataset["events"]) + 1
    delayed = next(event for event in dataset["events"]
                   if event["kind"] == "label" and event["prediction_event_id"] == first_feature["event_id"])
    delayed["ingested_at"] = iso_time(historical_cutoff + 3600)
    dataset["events"].sort(key=lambda event: parse_time(event["ingested_at"]))
    assert parse_time(delayed["available_at"]) < historical_cutoff < parse_time(delayed["ingested_at"])
    return dataset, features, delayed, historical_cutoff


def test_rolling_window_uses_emitted_alerts_and_preserves_prior_cooldown(tmp_path):
    dataset = generate_dataset(machines=1, sites=1, hours=1)
    features = causal_feature_rows(dataset["events"])
    model = UnitPredictor(features)
    as_of = parse_time(dataset["manifest"]["last_arrival_at"]) + 1
    with Store(tmp_path / "monitor.db") as store:
        fill_store(store, dataset, model)
        alerts = [json.loads(row["body"]) for row in store.db.execute("SELECT body FROM alerts")]
        window = sorted(features, key=lambda row: parse_time(row["available_at"]))[-1:]
        previous_alert = max(alerts, key=lambda row: parse_time(row["available_at"]))
        assert previous_alert["feature_id"] not in {row["feature_id"] for row in window}
        assert all(0 < parse_time(row["available_at"]) - parse_time(previous_alert["available_at"]) < store.config.alert_cooldown_seconds for row in window)
        labels = {event["prediction_event_id"]: event for event in dataset["events"] if event["kind"] == "label"}
        naive = evaluate_scores(
            [{"feature": feature, "label": labels[feature["event_id"]]} for feature in window],
            [0.9], model.threshold, store.config,
        )
        assert naive["alert_count"] == 1  # Reconstructing cooldown at this boundary is wrong.
        report = monitor_store(store, model, as_of=as_of, limit=1)
        assert report["performance"]["scored_features"] == 1
        assert report["performance"]["mature_labels"] == 1
        assert report["performance"]["observed_alerts"] == 0
        assert report["performance"]["false_alerts"] == 0
        assert report["performance"]["alert_precision"] is None


def test_future_labels_are_not_counted_even_when_already_persisted(tmp_path):
    dataset = generate_dataset(machines=1, sites=1, hours=1)
    features = causal_feature_rows(dataset["events"])
    model = UnitPredictor(features)
    last_feature_time = max(parse_time(feature["available_at"]) for feature in features)
    first_label_time = min(parse_time(event["available_at"]) for event in dataset["events"] if event["kind"] == "label")
    assert last_feature_time < first_label_time
    with Store(tmp_path / "monitor.db") as store:
        fill_store(store, dataset, model)
        assert store.counts()["labels"] > 0
        report = monitor_store(store, model, as_of=last_feature_time + 1)
        performance = report["performance"]
        assert performance["scored_features"] == len(features)
        assert performance["mature_labels"] == 0
        assert performance["pending_labels"] == len(features)
        assert performance["status"] == "awaiting_labels"
        assert performance["observed_alerts"] == 0
        assert performance["observed_failures"] == 0


def test_monitor_excludes_mature_label_until_its_actual_ingestion_time(tmp_path):
    dataset, features, delayed, cutoff = transport_delayed_label_fixture()
    model = UnitPredictor(features)
    with Store(tmp_path / "monitor.db") as store:
        fill_store(store, dataset, model)
        counts_before = store.counts()
        # The delayed label belongs to an actually emitted alert, so its early
        # inclusion would also contaminate alert-quality statistics.
        assert store.db.execute(
            "SELECT COUNT(*) FROM alerts a JOIN predictions p USING(feature_id) WHERE p.event_id=?",
            (delayed["prediction_event_id"],),
        ).fetchone()[0] == 1
        historical = monitor_store(store, model, as_of=cutoff)["performance"]
        assert historical["scored_features"] == len(features)
        assert historical["mature_labels"] == len(features) - 1
        assert historical["pending_labels"] == 1
        assert historical["observed_alerts"] == counts_before["alerts"] - 1
        arrived = monitor_store(store, model, as_of=parse_time(delayed["ingested_at"]))["performance"]
        assert arrived["mature_labels"] == len(features)
        assert arrived["pending_labels"] == 0
        assert arrived["observed_alerts"] == counts_before["alerts"]
        assert store.counts() == counts_before


def test_snapshot_hides_only_transport_delayed_label_until_ingestion(tmp_path):
    dataset, features, delayed, cutoff = transport_delayed_label_fixture()
    model = UnitPredictor(features)
    with Store(tmp_path / "monitor.db") as store:
        fill_store(store, dataset, model)
        before = {prediction["event_id"]: prediction
                  for prediction in store.snapshot(as_of=cutoff)["predictions"]}
        assert len(before) == len(features)
        assert before[delayed["prediction_event_id"]]["label"] is None
        assert all(prediction["label"] is not None for event_id, prediction in before.items()
                   if event_id != delayed["prediction_event_id"])
        after = {prediction["event_id"]: prediction
                 for prediction in store.snapshot(as_of=parse_time(delayed["ingested_at"]))["predictions"]}
        assert after[delayed["prediction_event_id"]]["label"] == delayed
        assert all(prediction["label"] is not None for prediction in after.values())


def test_detected_drift_does_not_promote_activate_or_mutate_pipeline(tmp_path):
    dataset = generate_dataset(machines=1, sites=1, hours=12)
    features = causal_feature_rows(dataset["events"])
    reference = copy.deepcopy(features)
    for feature in reference:
        feature["values"]["temperature_mean"] += 100.0
    model = UnitPredictor(reference)
    with Store(tmp_path / "monitor.db") as store:
        fill_store(store, dataset, model)
        counts_before = store.counts()
        deployment_before = store.deployment()
        report = monitor_store(store, model, as_of=parse_time(dataset["manifest"]["last_arrival_at"]) + 1)
        assert report["data_drift"]["status"] == "drift_detected"
        assert report["promotion_performed"] is False
        assert report["data_drift"]["promotion_performed"] is False
        assert store.counts() == counts_before
        assert store.deployment() == deployment_before
        assert store.db.execute("SELECT COUNT(*) FROM deployments").fetchone()[0] == 1

