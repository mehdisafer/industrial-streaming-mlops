import copy
import json
from pathlib import Path

import pytest

from industrial_mlops.config import PipelineConfig, load_config
from industrial_mlops.contracts import ContractError, iso_time, parse_time, validate_event
from industrial_mlops.simulator import generate_dataset


@pytest.fixture
def sensor():
    return next(event for event in generate_dataset(machines=1, sites=1, hours=1)["events"] if event["kind"] == "sensor")


def test_sensor_accepts_dict_and_json_without_coercion(sensor):
    assert validate_event(sensor) == sensor
    assert validate_event(json.dumps(sensor)) == sensor
    modified = {**sensor, "vibration_mm_s": "1.4"}
    with pytest.raises(ContractError, match="invalid_numeric"):
        validate_event(modified)


@pytest.mark.parametrize("field", ["target", "failure_at", "health_state", "future_label"])
def test_truth_cannot_enter_sensor_contract(sensor, field):
    with pytest.raises(ContractError, match="unexpected_field"):
        validate_event({**sensor, field: 1})


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"schema_version": "2"}, "incompatible_schema"),
        ({"temperature_c": True}, "invalid_numeric"),
        ({"temperature_c": 400}, "implausible_sensor"),
        ({"vibration_mm_s": -0.1}, "implausible_sensor"),
        ({"pressure_bar": None}, "invalid_numeric"),
        ({"machine_id": "../../outside"}, "invalid_identifier"),
        ({"event_time": "2026-01-01T00:00:00"}, "invalid_timestamp"),
        ({"event_time": "2026-01-01T01:00:00+01:00"}, "invalid_timestamp"),
    ],
)
def test_invalid_sensor_dispositions(sensor, change, reason):
    with pytest.raises(ContractError, match=reason):
        validate_event({**sensor, **change})


def test_missing_feature_and_wrong_unit_are_quarantined(sensor):
    bad = copy.deepcopy(sensor)
    del bad["vibration_mm_s"]
    with pytest.raises(ContractError, match="missing_field"):
        validate_event(bad)
    bad = copy.deepcopy(sensor)
    bad["units"]["temperature_c"] = "degF"
    with pytest.raises(ContractError, match="incompatible_units"):
        validate_event(bad)


def test_nonfinite_and_duplicate_json_keys_rejected(sensor):
    raw = json.dumps({**sensor, "temperature_c": float("nan")})
    with pytest.raises(ContractError, match="invalid_numeric"):
        validate_event(raw)
    raw = json.dumps(sensor)[:-1] + ', "temperature_c": 60}'
    with pytest.raises(ContractError, match="duplicate_json_key"):
        validate_event(raw)


def test_nonobjects_oversize_and_invalid_json_rejected():
    for value in ["[]", "null", 12, False]:
        with pytest.raises(ContractError, match="invalid_payload"):
            validate_event(value)
    with pytest.raises(ContractError, match="invalid_json"):
        validate_event('{"broken"')
    with pytest.raises(ContractError, match="oversized_payload"):
        validate_event("x" * 65537)


def test_deep_json_and_invalid_utf8_are_poison_records_not_uncaught_exceptions():
    with pytest.raises(ContractError, match="invalid_json"):
        validate_event("[" * 10000 + "0" + "]" * 10000)
    with pytest.raises(ContractError, match="invalid_utf8"):
        validate_event(chr(0xD800))


def test_labels_must_be_mature_and_within_horizon():
    generated = generate_dataset(machines=1, sites=1, hours=24)["events"]
    positive = next(event for event in generated if event["kind"] == "label" and event["target"] == 1)
    negative = next(event for event in generated if event["kind"] == "label" and event["target"] == 0)
    assert validate_event(positive) == positive
    assert validate_event(negative) == negative
    with pytest.raises(ContractError, match="unavailable_label"):
        validate_event({**positive, "available_at": positive["event_time"]})
    with pytest.raises(ContractError, match="unavailable_label"):
        validate_event({**negative, "available_at": negative["event_time"]})
    with pytest.raises(ContractError, match="unavailable_label"):
        validate_event({**positive, "ingested_at": positive["event_time"]})
    with pytest.raises(ContractError, match="invalid_target"):
        validate_event({**positive, "failure_at": iso_time(parse_time(positive["event_time"]) + 1801)})
    with pytest.raises(ContractError, match="invalid_target"):
        validate_event({**negative, "target": True})


def test_utc_roundtrip():
    text = "2026-03-01T12:34:56.123Z"
    assert iso_time(parse_time(text)) == text
    with pytest.raises(ContractError):
        parse_time("2026-02-30T00:00:00Z")


def test_validated_config_rejects_unreviewed_operations():
    assert load_config().feature_version == "v1"
    with pytest.raises(ValueError, match="Unknown configuration"):
        PipelineConfig.from_dict({"transform_python": "print('unsafe')"})
    for invalid in [{"lateness_seconds": -1}, {"min_points": True}, {"min_recall": 1.1}, {"max_p95_inference_ms": float("nan")}, {"feature_version": "v2"}]:
        with pytest.raises(ValueError):
            PipelineConfig.from_dict(invalid)
    with pytest.raises(ValueError, match="min_lead_seconds"):
        PipelineConfig(horizon_seconds=60, min_lead_seconds=61)
    config_file = Path(__file__).parents[1] / "config" / "pipeline.json"
    assert load_config(config_file) == PipelineConfig()


def test_generated_events_match_published_json_schemas():
    import jsonschema

    root = Path(__file__).parents[1] / "schemas"
    validators = {
        kind: jsonschema.Draft202012Validator(json.loads((root / f"{kind}.v1.json").read_text()), format_checker=jsonschema.FormatChecker())
        for kind in ("sensor", "label", "heartbeat")
    }
    for event in generate_dataset(machines=1, sites=1, hours=1)["events"]:
        validators[event["kind"]].validate(event)

