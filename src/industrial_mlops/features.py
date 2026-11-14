"""One causal feature definition shared by historical replay and the live sink.

The reference engine finalizes an anchor only after its machine watermark passes
it. Labels never advance a watermark. A late event is quarantined; finalized
features are immutable. This is a bounded local reference, not a distributed
state store or an end-to-end exactly-once claim.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from statistics import fmean

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import UNITS, ContractError, content_hash, iso_time, parse_time, validate_event

FEATURE_NAMES = (
    "temperature_mean", "temperature_max", "vibration_mean", "vibration_max",
    "pressure_mean", "temperature_slope", "vibration_slope", "sample_count",
)


def feature_signature(config: PipelineConfig | None = None) -> str:
    """Bind model compatibility to semantics, including target/availability rules."""
    config = config or PipelineConfig()
    return content_hash({
        "feature_version": config.feature_version,
        "feature_names": list(FEATURE_NAMES),
        "window_seconds": config.window_seconds,
        "min_points": config.min_points,
        "horizon_seconds": config.horizon_seconds,
        "lateness_seconds": config.lateness_seconds,
        "units": UNITS,
    })


def _slope(history: list[dict], field: str) -> float:
    """Ordinary least-squares slope per minute, without fitting across windows."""
    origin = parse_time(history[0]["event_time"])
    xs = [(parse_time(event["event_time"]) - origin) / 60.0 for event in history]
    ys = [float(event[field]) for event in history]
    x_mean, y_mean = fmean(xs), fmean(ys)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    if denominator == 0.0:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True)) / denominator


def machine_key(event: dict) -> tuple[str, str, str]:
    return event["dataset_id"], event["site_id"], event["machine_id"]


def build_features(
    history: list[dict],
    anchor: dict,
    config: PipelineConfig | None = None,
    as_of: float | None = None,
) -> dict | None:
    """Build a trailing (anchor-window, anchor] observation window.

    A source must also have arrived by ``as_of``. No labels, future observation,
    other machine or future-arriving observation can contribute. ``available_at``
    is the actual closure time supplied by the stream/replay engine, not the
    anchor time or last contributing source time.
    """
    config = config or PipelineConfig()
    if anchor.get("kind") != "sensor":
        raise ContractError("invalid_feature_anchor", "Only sensor events are eligible")
    anchor_time = parse_time(anchor["event_time"])
    as_of = parse_time(anchor["ingested_at"]) if as_of is None else as_of
    if parse_time(anchor["ingested_at"]) > as_of or anchor_time > as_of:
        return None
    sources: dict[str, dict] = {}
    for event in history:
        if event.get("kind") != "sensor" or machine_key(event) != machine_key(anchor):
            continue
        event_time = parse_time(event["event_time"])
        if not anchor_time - config.window_seconds < event_time <= anchor_time:
            continue
        if parse_time(event["ingested_at"]) > as_of:
            continue
        prior = sources.get(event["event_id"])
        if prior is not None and content_hash(prior) != content_hash(event):
            raise ContractError("conflicting_event_id", event["event_id"])
        sources[event["event_id"]] = event
    if anchor["event_id"] not in sources or len(sources) < config.min_points:
        return None
    ordered = sorted(sources.values(), key=lambda event: (parse_time(event["event_time"]), event["event_id"]))
    temperatures = [float(event["temperature_c"]) for event in ordered]
    vibrations = [float(event["vibration_mm_s"]) for event in ordered]
    source_event_ids = [event["event_id"] for event in ordered]
    identity = {
        "dataset_id": anchor["dataset_id"], "site_id": anchor["site_id"],
        "machine_id": anchor["machine_id"], "event_id": anchor["event_id"],
        "event_time": anchor["event_time"], "feature_version": config.feature_version,
        "feature_signature": feature_signature(config),
        "window_seconds": config.window_seconds, "source_event_ids": source_event_ids,
    }
    return {
        "feature_id": content_hash(identity),
        "event_id": anchor["event_id"],
        "dataset_id": anchor["dataset_id"],
        "site_id": anchor["site_id"],
        "machine_id": anchor["machine_id"],
        "event_time": anchor["event_time"],
        "available_at": iso_time(as_of),
        "feature_version": config.feature_version,
        "feature_signature": feature_signature(config),
        "values": {
            "temperature_mean": fmean(temperatures),
            "temperature_max": max(temperatures),
            "vibration_mean": fmean(vibrations),
            "vibration_max": max(vibrations),
            "pressure_mean": fmean(float(event["pressure_bar"]) for event in ordered),
            "temperature_slope": _slope(ordered, "temperature_c"),
            "vibration_slope": _slope(ordered, "vibration_mm_s"),
            "sample_count": len(ordered),
        },
        "source_event_ids": source_event_ids,
    }


def replay_with_report(events: list[dict | str], config: PipelineConfig | None = None) -> tuple[list[dict], dict]:
    """Reference arrival-order replay, independent of Spark batch boundaries.

    Ordering within a machine must match its Kafka partition order. Cross-machine
    reordering has no effect. The report has one mutually exclusive disposition
    for every source record, including malformed text and duplicate labels.
    """
    config = config or PipelineConfig()
    seen: dict[str, str] = {}
    accepted_sensors: dict[str, dict] = {}
    labeled_anchors: set[str] = set()
    max_seen: dict[tuple, float] = {}
    history: dict[tuple, list[dict]] = defaultdict(list)
    pending: dict[tuple, list[dict]] = defaultdict(list)
    rows: list[dict] = []
    counts: Counter = Counter()
    reasons: Counter = Counter()
    for raw in events:
        counts["source"] += 1
        try:
            event = validate_event(raw, config)
        except ContractError as error:
            counts["rejected"] += 1
            reasons[error.reason] += 1
            continue
        digest = content_hash(event)
        previous = seen.get(event["event_id"])
        if previous is not None:
            if previous == digest:
                counts["duplicates"] += 1
            else:
                counts["rejected"] += 1
                reasons["conflicting_event_id"] += 1
            continue
        seen[event["event_id"]] = digest
        key = machine_key(event)
        event_time = parse_time(event["event_time"])
        prior_watermark = max_seen.get(key, float("-inf")) - config.lateness_seconds
        if event["kind"] == "sensor" and event_time <= prior_watermark:
            counts["rejected"] += 1
            reasons["late_after_watermark"] += 1
            continue
        if event["kind"] == "label":
            target = accepted_sensors.get(event["prediction_event_id"])
            reason = None
            if target is None:
                reason = "label_target_not_accepted"
            elif any(event[field] != target[field] for field in (
                "dataset_id", "site_id", "machine_id", "event_time",
            )):
                reason = "label_target_identity_conflict"
            elif event["prediction_event_id"] in labeled_anchors:
                reason = "label_target_already_exists"
            if reason is not None:
                counts["rejected"] += 1
                reasons[reason] += 1
                continue
            labeled_anchors.add(event["prediction_event_id"])
        counts["accepted"] += 1
        counts[f"accepted_{event['kind']}"] += 1
        if event["kind"] == "label":
            continue
        if event["kind"] == "sensor":
            accepted_sensors[event["event_id"]] = event
            history[key].append(event)
            pending[key].append(event)
        max_seen[key] = max(event_time, max_seen.get(key, float("-inf")))
        watermark = max_seen[key] - config.lateness_seconds
        ready = sorted(
            (event for event in pending[key] if parse_time(event["event_time"]) <= watermark),
            key=lambda event: (event["event_time"], event["event_id"]),
        )
        ready_ids = {event["event_id"] for event in ready}
        for anchor in ready:
            feature = build_features(history[key], anchor, config, as_of=parse_time(event["ingested_at"]))
            if feature is None:
                counts["warmup"] += 1
            else:
                rows.append(feature)
        pending[key] = [event for event in pending[key] if event["event_id"] not in ready_ids]
        # Keep the full trailing window needed by the earliest unfinalized anchor.
        cutoff = watermark - config.window_seconds
        history[key] = [event for event in history[key] if parse_time(event["event_time"]) > cutoff]
    counts["features"] = len(rows)
    counts["pending"] = sum(len(items) for items in pending.values())
    for name in ("source", "accepted", "rejected", "duplicates", "warmup", "pending"):
        counts.setdefault(name, 0)
    return rows, {"counts": dict(counts), "rejection_reasons": dict(reasons)}


def causal_feature_rows(events: list[dict | str], config: PipelineConfig | None = None) -> list[dict]:
    return replay_with_report(events, config)[0]


replay_features = causal_feature_rows

