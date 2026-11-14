"""Strict sensor, delayed-label and heartbeat contracts, schema version 1."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import re

from industrial_mlops.config import PipelineConfig

UNITS = {
    "temperature_c": "degC",
    "vibration_mm_s": "mm/s",
    "pressure_bar": "bar",
}
COMMON_FIELDS = frozenset(
    {
        "schema_version", "kind", "event_id", "dataset_id", "source",
        "site_id", "machine_id", "event_time", "ingested_at",
    }
)
KIND_FIELDS = {
    "sensor": frozenset({"temperature_c", "vibration_mm_s", "pressure_bar", "units"}),
    "label": frozenset({"prediction_event_id", "target", "failure_at", "available_at"}),
    "heartbeat": frozenset(),
}
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$")


class ContractError(ValueError):
    """A stable quarantine reason and a human-readable explanation."""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


def parse_time(value: str) -> float:
    """Read a UTC RFC3339 timestamp, rejecting naive or non-UTC timestamps."""
    if not isinstance(value, str) or not UTC_TIMESTAMP.fullmatch(value):
        raise ContractError("invalid_timestamp", "Expected an explicit UTC RFC3339 timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, OverflowError, OSError) as error:
        raise ContractError("invalid_timestamp", str(error)) from error
    if not math.isfinite(result):
        raise ContractError("invalid_timestamp", "Non-finite timestamp")
    return result


def iso_time(value: float) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContractError("invalid_timestamp", "Expected finite Unix seconds")
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def content_hash(value: dict | str) -> str:
    content = value if isinstance(value, str) else canonical_json(value)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ContractError("duplicate_json_key", str(key))
        value[key] = item
    return value


def _number(value: object, name: str, low: float, high: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContractError("invalid_numeric", name)
    if not low <= value <= high:
        raise ContractError("implausible_sensor", f"{name} outside [{low}, {high}]")


def validate_event(raw: str | dict, config: PipelineConfig | None = None) -> dict:
    """Validate without coercing types or letting target information into sensors."""
    config = config or PipelineConfig()
    if isinstance(raw, str):
        try:
            encoded_length = len(raw.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ContractError("invalid_utf8", "Payload is not valid UTF-8 text") from error
        if encoded_length > config.max_payload_bytes:
            raise ContractError("oversized_payload")
        try:
            event = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, RecursionError) as error:
            raise ContractError("invalid_json", str(error)) from error
    elif isinstance(raw, dict):
        event = raw.copy()
        try:
            if len(canonical_json(event).encode("utf-8")) > config.max_payload_bytes:
                raise ContractError("oversized_payload")
        except (TypeError, ValueError, RecursionError) as error:
            if isinstance(error, ContractError):
                raise
            raise ContractError("invalid_json_value", str(error)) from error
    else:
        raise ContractError("invalid_payload", "Expected a JSON object or UTF-8 JSON text")
    if not isinstance(event, dict):
        raise ContractError("invalid_payload", "Top-level JSON must be an object")
    if event.get("schema_version") != "1":
        raise ContractError("incompatible_schema", str(event.get("schema_version")))
    kind = event.get("kind")
    if not isinstance(kind, str) or kind not in KIND_FIELDS:
        raise ContractError("unknown_event_kind", str(kind))
    required = COMMON_FIELDS | KIND_FIELDS[kind]
    missing = required - event.keys()
    if missing:
        raise ContractError("missing_field", ",".join(sorted(missing)))
    unexpected = event.keys() - required
    if unexpected:
        raise ContractError("unexpected_field", ",".join(sorted(unexpected)))
    for name in ("event_id", "dataset_id", "source", "site_id", "machine_id"):
        if not isinstance(event[name], str) or not IDENTIFIER.fullmatch(event[name]):
            raise ContractError("invalid_identifier", name)
    event_time = parse_time(event["event_time"])
    ingested_at = parse_time(event["ingested_at"])
    if ingested_at < event_time:
        raise ContractError("invalid_availability", "ingested_at precedes event_time")
    if kind == "sensor":
        if event["units"] != UNITS:
            raise ContractError("incompatible_units")
        _number(event["temperature_c"], "temperature_c", config.temperature_min_c, config.temperature_max_c)
        _number(event["vibration_mm_s"], "vibration_mm_s", 0.0, config.vibration_max_mm_s)
        _number(event["pressure_bar"], "pressure_bar", 0.0, config.pressure_max_bar)
    elif kind == "label":
        if not isinstance(event["prediction_event_id"], str) or not IDENTIFIER.fullmatch(event["prediction_event_id"]):
            raise ContractError("invalid_identifier", "prediction_event_id")
        if type(event["target"]) is not int or event["target"] not in (0, 1):
            raise ContractError("invalid_target")
        available_at = parse_time(event["available_at"])
        if ingested_at < available_at:
            raise ContractError("unavailable_label", "Label arrived before available_at")
        if event["target"] == 1:
            if event["failure_at"] is None:
                raise ContractError("invalid_target", "Positive label requires failure_at")
            failure_at = parse_time(event["failure_at"])
            if not event_time < failure_at <= event_time + config.horizon_seconds:
                raise ContractError("invalid_target", "Failure outside the prediction horizon")
            if available_at < failure_at:
                raise ContractError("unavailable_label", "Label known before failure")
        else:
            if event["failure_at"] is not None:
                raise ContractError("invalid_target", "Negative label must have null failure_at")
            if available_at < event_time + config.horizon_seconds:
                raise ContractError("unavailable_label", "Negative horizon has not elapsed")
    return event

