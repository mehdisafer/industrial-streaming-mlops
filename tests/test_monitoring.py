from copy import deepcopy

import pytest

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import iso_time
from industrial_mlops.features import FEATURE_NAMES
from industrial_mlops.monitoring import delayed_performance, drift_report, prediction_shift, reference_profile


def features(count=200):
    return [{"feature_id": f"f-{i}", "event_id": f"e-{i}", "dataset_id": "test", "site_id": "s", "machine_id": "m",
             "event_time": iso_time(1000 + i * 300), "available_at": iso_time(1310 + i * 300),
             "values": {name: float(1 + i % 20) for name in FEATURE_NAMES}, "feature_version": "v1"} for i in range(count)]


def test_stable_then_shifted_features_are_observed_without_promotion():
    original = features()
    profile = reference_profile(original)
    stable = drift_report(profile, original)
    assert stable["status"] == "within_reference"
    shifted = deepcopy(original)
    for f in shifted:
        f["values"]["vibration_mean"] += 100
    report = drift_report(profile, shifted)
    assert report["status"] == "drift_detected"
    assert report["features"]["vibration_mean"]["drifted"]
    assert report["promotion_performed"] is False
    assert drift_report(profile, original[:5])["status"] == "insufficient_data"


def test_prediction_shift_has_explicit_window_sizes_and_no_mutation():
    report = prediction_shift([0.1, 0.2, 0.3], [0.5, 0.9], 0.5)
    assert report["reference_count"] == 3 and report["current_count"] == 2
    assert report["current_positive_rate"] == 1
    assert report["promotion_performed"] is False
    with pytest.raises(ValueError):
        prediction_shift([0.1], [float("nan")], 0.5)


def test_performance_waits_for_label_availability():
    observed = features(1)
    f = observed[0]
    label = {key: f[key] for key in ("dataset_id", "site_id", "machine_id", "event_time")}
    label.update(kind="label", prediction_event_id=f["event_id"], target=1, failure_at=iso_time(2000),
                 available_at=iso_time(5000), ingested_at=iso_time(5000))
    before = delayed_performance(observed, [label], {f["feature_id"]: 0.9}, 0.5, PipelineConfig(), as_of=4999)
    assert before["status"] == "awaiting_labels" and before["metrics"] is None
    after = delayed_performance(observed, [label], {f["feature_id"]: 0.9}, 0.5, PipelineConfig(), as_of=5000)
    assert after["metrics"]["true_alerts"] == 1
    assert after["coverage"]["available_labels"] == 1
