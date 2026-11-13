from collections import Counter

import pytest

from industrial_mlops.contracts import parse_time
from industrial_mlops.features import replay_with_report
from industrial_mlops.simulator import generate_dataset


def test_generation_is_deterministic_and_identity_covers_configuration():
    options = {"machines": 2, "sites": 2, "hours": 2, "anomalies": True}
    first = generate_dataset(**options)
    assert first == generate_dataset(**options)
    assert first["manifest"]["dataset_id"] != generate_dataset(**{**options, "seed": 43})["manifest"]["dataset_id"]
    assert first["manifest"]["dataset_id"] != generate_dataset(**{**options, "drift": 1.0})["manifest"]["dataset_id"]


def test_all_generated_sources_reconcile_with_independent_manifest_counts():
    dataset = generate_dataset(machines=3, sites=2, hours=24, anomalies=True)
    _, report = replay_with_report(dataset["events"])
    for name, expected in dataset["manifest"]["expected_dispositions"].items():
        assert report["counts"][name] == expected, name
    assert report["counts"]["pending"] == 0
    assert report["counts"]["source"] == report["counts"]["accepted"] + report["counts"]["rejected"] + report["counts"]["duplicates"]
    assert report["rejection_reasons"] == dataset["manifest"]["injected_anomalies"]["rejects"]


def test_ground_truth_is_separate_mature_and_not_leaked_to_sensors():
    dataset = generate_dataset(machines=2, sites=2, hours=72)
    sensors = {event["event_id"]: event for event in dataset["events"] if event["kind"] == "sensor"}
    labels = [event for event in dataset["events"] if event["kind"] == "label"]
    assert len(sensors) == len(labels)
    counts = Counter(label["target"] for label in labels)
    assert counts[1] > 12 and counts[0] > counts[1]
    for event in sensors.values():
        assert not ({"target", "failure_at", "severity", "health_state"} & event.keys())
    for label in labels:
        assert label["prediction_event_id"] in sensors
        anchor = parse_time(label["event_time"])
        available = parse_time(label["available_at"])
        assert available <= parse_time(label["ingested_at"])
        if label["target"]:
            failure = parse_time(label["failure_at"])
            assert anchor < failure <= anchor + 1800
            assert available >= failure + 3600
        else:
            assert available >= anchor + 1800 + 3600


def test_clean_generator_is_arrival_ordered():
    events = generate_dataset(machines=2, sites=2, hours=3)["events"]
    arrivals = [parse_time(event["ingested_at"]) for event in events]
    assert arrivals == sorted(arrivals)


@pytest.mark.parametrize("options", [{"seed": -1}, {"machines": 0}, {"hours": 0}, {"drift": float("nan")}, {"anomalies": "yes"}, {"machines": 1000, "sites": 2, "hours": 720, "step_seconds": 1}])
def test_generator_is_bounded_and_validates_parameters(options):
    with pytest.raises(ValueError):
        generate_dataset(**options)

