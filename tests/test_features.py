import copy
import json
from pathlib import Path

import pytest

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import UNITS, iso_time, parse_time
from industrial_mlops.features import FEATURE_NAMES, build_features, causal_feature_rows, feature_signature, replay_with_report
from industrial_mlops.simulator import generate_dataset

START = parse_time("2026-01-01T00:00:00Z")


def sensor(second, *, ingestion=None, event_id=None, temperature=60.0, machine="m1", dataset="d1"):
    return {
        "schema_version": "1", "kind": "sensor", "event_id": event_id or f"e{second}",
        "dataset_id": dataset, "source": "test", "site_id": "s1", "machine_id": machine,
        "event_time": iso_time(START + second), "ingested_at": iso_time(START + (second if ingestion is None else ingestion)),
        "temperature_c": temperature, "vibration_mm_s": 1.0 + second / 600.0,
        "pressure_bar": 6.0, "units": UNITS.copy(),
    }


def heartbeat(second, *, machine="m1"):
    event = sensor(second, machine=machine, event_id=f"heartbeat-{second}-{machine}")
    event["kind"] = "heartbeat"
    for field in ("temperature_c", "vibration_mm_s", "pressure_bar", "units"):
        del event[field]
    return event


def test_features_ignore_future_labels_other_machines_and_late_availability():
    observations = [sensor(second, temperature=60 + second / 60) for second in (0, 300, 600, 900)]
    anchor = observations[-1]
    baseline = build_features(observations, anchor)
    contaminants = [
        sensor(1200, temperature=170),
        sensor(700, ingestion=1500, event_id="not-yet-known", temperature=170),
        sensor(800, event_id="other-machine", machine="m2", temperature=170),
        sensor(800, event_id="other-dataset", dataset="d2", temperature=170),
        {"kind": "label", "target": 1, "failure_at": "2030-01-01T00:00:00Z"},
    ]
    result = build_features(observations + contaminants, anchor)
    assert result == baseline
    assert tuple(result["values"]) == FEATURE_NAMES
    assert "target" not in result["values"]
    assert result["values"]["temperature_slope"] == pytest.approx(1.0)
    assert result["values"]["vibration_slope"] == pytest.approx(0.1)
    assert result["source_event_ids"] == [event["event_id"] for event in observations]


def test_window_left_boundary_is_open_and_available_time_is_actual_closure():
    cfg = PipelineConfig(window_seconds=600, min_points=2)
    observations = [sensor(second) for second in (0, 300, 600)]
    result = build_features(observations, observations[-1], cfg, as_of=START + 721)
    assert result["source_event_ids"] == ["e300", "e600"]
    assert result["available_at"] == iso_time(START + 721)
    assert result["values"]["sample_count"] == 2
    assert build_features(observations, observations[-1], cfg, as_of=START + 599) is None


def test_warmup_and_missing_anchor_do_not_emit_incomplete_feature():
    observations = [sensor(second) for second in (0, 300, 600)]
    assert build_features(observations, observations[-1]) is None
    assert build_features(observations, sensor(900)) is None


def test_feature_signature_changes_with_semantics_but_not_operating_budget():
    base = feature_signature()
    for changed in [PipelineConfig(window_seconds=3600), PipelineConfig(min_points=5), PipelineConfig(horizon_seconds=3600), PipelineConfig(lateness_seconds=60)]:
        assert feature_signature(changed) != base
    assert feature_signature(PipelineConfig(false_alert_budget_per_machine_day=1.0)) == base


def test_exact_duplicate_never_changes_feature_values():
    observations = [sensor(second) for second in (0, 300, 600, 900)]
    assert build_features(observations + [copy.deepcopy(observations[0])], observations[-1]) == build_features(observations, observations[-1])


def test_late_watermark_boundary_is_rejected_but_in_window_disorder_is_accepted():
    cfg = PipelineConfig(min_points=2)
    events = [sensor(0), sensor(300), sensor(180, ingestion=301), sensor(240, ingestion=302), heartbeat(600)]
    rows, report = replay_with_report(events, cfg)
    assert report["rejection_reasons"] == {"late_after_watermark": 1}
    assert report["counts"]["accepted_sensor"] == 3
    by_event = {row["event_id"]: row for row in rows}
    assert by_event["e240"]["source_event_ids"] == ["e0", "e240"]
    assert by_event["e300"]["source_event_ids"] == ["e0", "e240", "e300"]
    assert all(row["available_at"] == iso_time(START + 600) for row in rows)


def test_cross_machine_partition_order_does_not_change_features():
    dataset = generate_dataset(machines=3, sites=2, hours=3, anomalies=True)
    original = causal_feature_rows(dataset["events"])
    # Each machine retains its arrival order, as it does with a stable Kafka key.
    reordered = sorted(dataset["events"], key=lambda event: (event.get("machine_id", "") if isinstance(event, dict) else ""))
    partitioned = causal_feature_rows(reordered)
    assert sorted(original, key=lambda row: row["feature_id"]) == sorted(partitioned, key=lambda row: row["feature_id"])


def test_labels_cannot_close_windows_or_change_features():
    dataset = generate_dataset(machines=1, sites=1, hours=2)
    rows = causal_feature_rows(dataset["events"])
    no_labels = [event for event in dataset["events"] if event["kind"] != "label"]
    assert causal_feature_rows(no_labels) == rows
    no_closure = [event for event in dataset["events"] if event["kind"] == "sensor"]
    _, report = replay_with_report(no_closure)
    assert report["counts"]["pending"] > 0


def test_feature_output_matches_versioned_json_schema():
    import jsonschema

    schema = json.loads((Path(__file__).parents[1] / "schemas" / "feature.v1.json").read_text())
    validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    for row in causal_feature_rows(generate_dataset(machines=1, sites=1, hours=1)["events"]):
        validator.validate(row)

