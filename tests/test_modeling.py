from copy import deepcopy

import numpy as np
import pytest

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import iso_time
from industrial_mlops.features import FEATURE_NAMES
from industrial_mlops.modeling import (
    assert_temporal_separation, choose_threshold, evaluate_scores, feature_frame,
    fit_models, labeled_rows, qualification,
)


def row(index=0, *, target=0, time=1000, available=None, failure=None, dataset="dataset-a"):
    feature = {
        "feature_id": f"feature-{index}", "event_id": f"event-{index}",
        "dataset_id": dataset, "site_id": "site-a", "machine_id": "machine-a",
        "event_time": iso_time(time), "available_at": iso_time(time if available is None else available),
        "feature_version": "v1", "values": {name: float(index + 1) for name in FEATURE_NAMES},
        "source_event_ids": [f"source-{index}"],
    }
    label = {"kind": "label", "prediction_event_id": feature["event_id"], "target": target,
             "available_at": iso_time(time + 3600), "ingested_at": iso_time(time + 3600),
             "failure_at": iso_time(failure) if failure is not None else None}
    label.update({key: feature[key] for key in ("dataset_id", "site_id", "machine_id", "event_time")})
    return {"feature": feature, "label": label}


@pytest.mark.parametrize("mutation", ["missing", "extra", "nan", "bool"])
def test_model_feature_contract_rejects_invalid_values(mutation):
    feature = row()["feature"]
    if mutation == "missing":
        feature["values"].pop(FEATURE_NAMES[0])
    elif mutation == "extra":
        feature["values"]["failure_target"] = 1
    elif mutation == "nan":
        feature["values"][FEATURE_NAMES[0]] = float("nan")
    else:
        feature["values"][FEATURE_NAMES[0]] = True
    with pytest.raises(ValueError):
        feature_frame([feature])


def test_labels_must_be_available_and_ingested_without_identity_mismatch():
    sample = row()
    feature, label = sample["feature"], sample["label"]
    assert labeled_rows([feature], [label], as_of=4599)[0] == []
    label["ingested_at"] = iso_time(4700)
    assert labeled_rows([feature], [label], as_of=4699)[0] == []
    assert len(labeled_rows([feature], [label], as_of=4700)[0]) == 1
    label["machine_id"] = "another-machine"
    with pytest.raises(ValueError, match="identity mismatch"):
        labeled_rows([feature], [label], as_of=4700)


def test_temporal_split_respects_delayed_label_availability():
    train = [row(time=1000, dataset="train")]
    validation = [row(time=4500, dataset="validation")]
    with pytest.raises(ValueError, match="unavailable labels"):
        assert_temporal_separation(train, validation)
    validation = [row(time=5000, dataset="validation")]
    assert_temporal_separation(train, validation)
    validation[0]["feature"]["dataset_id"] = "train"
    with pytest.raises(ValueError, match="identity is reused"):
        assert_temporal_separation(train, validation)


def test_scaler_is_fitted_only_on_training_rows():
    training = [row(i, target=i % 2) for i in range(20)]
    model = fit_models(training)["candidate"]
    before = model.named_steps["scale"].mean_.copy()
    np.testing.assert_allclose(before, np.full(len(FEATURE_NAMES), 10.5))
    validation = deepcopy(training)
    for sample in validation:
        sample["feature"]["values"] = {name: 100000.0 for name in FEATURE_NAMES}
    model.predict_proba(feature_frame([r["feature"] for r in validation]))
    np.testing.assert_array_equal(model.named_steps["scale"].mean_, before)


def test_cooldown_and_actual_availability_control_alert_quality():
    rows = [row(0, target=1, time=1000, available=1100, failure=2000),
            row(1, target=1, time=1200, available=1300, failure=2000),
            row(2, target=1, time=3000, available=4100, failure=4000),
            row(3, target=0, time=86400)]
    metrics = evaluate_scores(rows, [0.9, 0.9, 0.9, 0.1], 0.5, PipelineConfig())
    assert metrics["event_recall"] == 1.0
    assert metrics["alert_count"] == 2  # repeated positive is suppressed
    assert metrics["true_alerts"] == 1
    assert metrics["false_alerts"] == 1  # prediction available after failure
    assert metrics["failure_recall"] == 0.5
    assert metrics["lead_seconds_median"] == 900


def test_threshold_uses_validation_budget_and_cooldown():
    samples = [row(0, target=1, time=1000, available=1100, failure=2000),
               row(1, target=0, time=4000), row(2, target=0, time=8000),
               row(3, target=0, time=86400)]
    threshold, metrics = choose_threshold(samples, [0.9, 0.7, 0.4, 0.1],
                                         PipelineConfig(false_alert_budget_per_machine_day=0))
    assert 0.7 < threshold <= 0.9
    assert metrics["false_alerts"] == 0
    assert metrics["useful_failure_recall"] == 1.0


def test_tied_availability_uses_same_event_time_order_as_live_sink():
    early = row(9, target=0, time=1000, available=1400)
    later = row(0, target=1, time=1200, available=1400, failure=2000)
    metrics = evaluate_scores([later, early], [0.9, 0.9], 0.5, PipelineConfig())
    assert metrics["alerts"][0]["event_id"] == early["feature"]["event_id"]
    assert metrics["false_alerts"] == 1 and metrics["detected_failures"] == 0


def test_candidate_promotion_needs_strict_gain_and_evidence():
    config = PipelineConfig()
    metrics = {"observed_failures": 8, "machine_days": 10,
               "false_alerts_per_machine_day": 0.5, "useful_failure_recall": 0.9,
               "lead_seconds_median": 600}
    assert qualification(metrics, {"p95_ms": 20}, config)["eligible"]
    assert not qualification(metrics, {"p95_ms": 20}, config, baseline=metrics)["eligible"]
    improved = dict(metrics, false_alerts_per_machine_day=0.25)
    assert qualification(improved, {"p95_ms": 20}, config, baseline=metrics)["eligible"]
    assert not qualification(improved, {"p95_ms": 2000}, config, baseline=metrics)["eligible"]
    assert not qualification(improved, {"p95_ms": 20}, config, label_coverage=0.5)["eligible"]
