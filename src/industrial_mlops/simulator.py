"""Deterministic, synthetic-only industrial telemetry and delayed ground truth.

The failure schedule is intentionally kept outside sensor payloads. The model
sees noisy physical measurements; labels arrive separately after their outcome
can be known. Synthetic predictability is not evidence of real-plant accuracy.
"""

from __future__ import annotations

import copy
import math
import random

from industrial_mlops.contracts import UNITS, canonical_json, content_hash, iso_time, parse_time

SIMULATOR_VERSION = "1.0.0"


def generate_dataset(
    seed: int = 42,
    machines: int = 6,
    sites: int = 2,
    hours: int = 72,
    start: str = "2026-01-01T00:00:00Z",
    step_seconds: int = 300,
    anomalies: bool = False,
    drift: float = 0.0,
    label_delay_seconds: int = 3600,
    horizon_seconds: int = 1800,
    lateness_seconds: int = 120,
) -> dict:
    """Return arrival-ordered events and a provenance/ground-truth manifest.

    Every original sensor has one delayed label. Failure and normal recovery are
    both represented; short healthy load spikes create competing explanations.
    ``drift`` shifts measurements halfway through the observation period. With
    ``anomalies=True`` there is one in-window out-of-order sensor (and label),
    one exact duplicate, and six explicit rejects. This finite fixture uses a
    reviewed heartbeat to close its final windows; live sensor traffic advances
    the same watermark naturally.
    """
    bounds = {
        "seed": (seed, 0, 2**31 - 1),
        "machines": (machines, 1, 1000),
        "sites": (sites, 1, machines),
        "hours": (hours, 1, 24 * 365),
        "step_seconds": (step_seconds, 1, 1800),
        "label_delay_seconds": (label_delay_seconds, 0, 86400),
        "horizon_seconds": (horizon_seconds, 60, 86400),
        "lateness_seconds": (lateness_seconds, 0, 3600),
    }
    for name, (value, low, high) in bounds.items():
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{name} must be an integer in [{low}, {high}]")
    if type(anomalies) is not bool:
        raise ValueError("anomalies must be a boolean")
    if anomalies and lateness_seconds == 0:
        raise ValueError("The within-watermark disorder fixture requires positive lateness_seconds")
    if isinstance(drift, bool) or not isinstance(drift, (int, float)) or not math.isfinite(drift) or not -3 <= drift <= 3:
        raise ValueError("drift must be finite and in [-3, 3]")
    duration = hours * 3600
    sample_count = len(range(0, duration, step_seconds))
    if sample_count * machines > 1000000:
        raise ValueError("A generated dataset is bounded to 1,000,000 sensor events")
    start_time = parse_time(start)
    parameters = {
        "simulator_version": SIMULATOR_VERSION, "seed": seed, "machines": machines,
        "sites": sites, "hours": hours, "start": iso_time(start_time),
        "step_seconds": step_seconds, "anomalies": anomalies, "drift": drift,
        "label_delay_seconds": label_delay_seconds, "horizon_seconds": horizon_seconds,
        "lateness_seconds": lateness_seconds,
    }
    dataset_id = f"synthetic-{content_hash(parameters)[:24]}"
    scheduled: list[tuple[float, int, dict | str]] = []
    sensors: list[dict] = []
    failures: dict[str, list[float]] = {}
    positive_count = 0

    def add(event: dict | str, arrival: float | None = None) -> None:
        if arrival is None:
            if not isinstance(event, dict):
                raise ValueError("Text records require an arrival time")
            arrival = parse_time(event["ingested_at"])
        scheduled.append((arrival, len(scheduled), event))

    def envelope(kind: str, event_id: str, machine: int, event_time: float, ingested_at: float) -> dict:
        return {
            "schema_version": "1", "kind": kind, "event_id": event_id,
            "dataset_id": dataset_id, "source": f"industrial-simulator-{SIMULATOR_VERSION}",
            "site_id": f"site-{machine % sites:02d}", "machine_id": f"machine-{machine:03d}",
            "event_time": iso_time(event_time), "ingested_at": iso_time(ingested_at),
        }

    def label_for(sensor: dict, machine: int) -> dict:
        anchor_time = parse_time(sensor["event_time"])
        future = next((time for time in failures[sensor["machine_id"]] if anchor_time < time <= anchor_time + horizon_seconds), None)
        mature = (future if future is not None else anchor_time + horizon_seconds) + label_delay_seconds
        label = envelope("label", f"label-{sensor['event_id']}", machine, anchor_time, mature)
        label.update({
            "prediction_event_id": sensor["event_id"], "target": int(future is not None),
            "failure_at": iso_time(future) if future is not None else None,
            "available_at": iso_time(mature),
        })
        return label

    for machine in range(machines):
        rng = random.Random(seed * 1000003 + machine * 9176)
        machine_id = f"machine-{machine:03d}"
        # Include the forecast horizon beyond the sensor period so trailing
        # labels are observed outcomes, rather than right-censored negatives.
        failure_times = []
        elapsed = rng.uniform(9.0, 13.0) * 3600
        while elapsed <= duration + horizon_seconds:
            failure_times.append(start_time + elapsed)
            elapsed += rng.uniform(16.0, 22.0) * 3600
        failures[machine_id] = failure_times
        temp_bias = rng.uniform(-4.0, 4.0)
        vibration_bias = rng.uniform(-0.2, 0.25)
        pressure_bias = rng.uniform(-0.3, 0.3)
        phase = rng.uniform(0, 2 * math.pi)
        # A handful of high-load episodes do not end in failure. These produce
        # transient temperature/vibration increases without the same slope.
        transient_centers = [rng.uniform(1.0, max(1.01, hours - 1.0)) * 3600 for _ in range(max(1, hours // 8))]
        for ordinal, offset in enumerate(range(0, duration, step_seconds)):
            event_time = start_time + offset
            upcoming = next((time for time in failure_times if time > event_time), None)
            time_to_failure = upcoming - event_time if upcoming is not None else float("inf")
            severity = max(0.0, 1.0 - time_to_failure / 5400.0)
            previous = next((time for time in reversed(failure_times) if time <= event_time), None)
            recovery = max(0.0, 1.0 - (event_time - previous) / 900.0) if previous is not None else 0.0
            transient = max((max(0.0, 1.0 - abs(offset - center) / 900.0) for center in transient_centers), default=0.0)
            drift_now = drift if offset >= duration / 2 else 0.0
            temperature = (
                58.0 + temp_bias + 2.0 * math.sin(offset / 3600 * math.pi / 12 + phase)
                + 30.0 * severity + 20.0 * recovery + 17.0 * transient
                + drift_now * 5.0 + rng.gauss(0.0, 1.2)
            )
            vibration = (
                1.45 + vibration_bias + 5.3 * severity**1.5 + 4.0 * recovery
                + 2.5 * transient + drift_now * 0.5 + rng.gauss(0.0, 0.13)
            )
            pressure = (
                6.2 + pressure_bias - 1.4 * severity - 1.6 * recovery
                + 0.5 * transient - drift_now * 0.2 + rng.gauss(0.0, 0.12)
            )
            ingested_at = event_time + rng.uniform(0.1, min(20.0, step_seconds * 0.2))
            sensor = envelope("sensor", f"{dataset_id}-m{machine:03d}-e{ordinal:07d}", machine, event_time, ingested_at)
            sensor.update({
                "temperature_c": round(temperature, 6),
                "vibration_mm_s": round(max(0.0, vibration), 6),
                "pressure_bar": round(max(0.0, pressure), 6), "units": UNITS.copy(),
            })
            sensors.append(sensor)
            add(sensor)
            label = label_for(sensor, machine)
            positive_count += label["target"]
            add(label)
        final_time = start_time + (sample_count - 1) * step_seconds + lateness_seconds + 1.0
        add(envelope("heartbeat", f"{dataset_id}-m{machine:03d}-heartbeat", machine, final_time, final_time))

    injected = {}
    if anomalies:
        # The parent machine already has a higher observation before this
        # extra sensor arrives, but its timestamp is still above the watermark.
        reference = sensors[min(12, sample_count - 1)]
        reference_time = parse_time(reference["event_time"])
        arrival = parse_time(reference["ingested_at"]) + 0.01
        out_of_order = copy.deepcopy(reference)
        out_of_order["event_id"] += "-ooo"
        out_of_order["event_time"] = iso_time(reference_time - min(lateness_seconds / 2, step_seconds / 2))
        out_of_order["ingested_at"] = iso_time(arrival)
        add(out_of_order)
        label = label_for(out_of_order, 0)
        add(label)
        positive_count += label["target"]
        duplicate = copy.deepcopy(reference)
        add(duplicate, arrival + 0.01)
        add('{"kind": "sensor", broken-json', arrival + 0.02)
        wrong_schema = copy.deepcopy(reference)
        wrong_schema.update(event_id=f"{reference['event_id']}-schema", schema_version="99")
        add(wrong_schema, arrival + 0.03)
        missing = copy.deepcopy(reference)
        missing["event_id"] += "-missing"
        del missing["vibration_mm_s"]
        add(missing, arrival + 0.04)
        conflict = copy.deepcopy(reference)
        conflict["temperature_c"] += 0.5
        add(conflict, arrival + 0.05)
        wrong_units = copy.deepcopy(reference)
        wrong_units["event_id"] += "-units"
        wrong_units["units"]["temperature_c"] = "degF"
        add(wrong_units, arrival + 0.06)
        late = copy.deepcopy(sensors[0])
        late["event_id"] += "-late"
        late["ingested_at"] = iso_time(start_time + duration + horizon_seconds + label_delay_seconds)
        add(late)
        injected = {
            "accepted_out_of_order_sensors": 1, "accepted_extra_labels": 1,
            "exact_duplicates": 1,
            "rejects": {
                "invalid_json": 1, "incompatible_schema": 1, "missing_field": 1,
                "conflicting_event_id": 1, "incompatible_units": 1, "late_after_watermark": 1,
            },
        }
    events = [event for _, _, event in sorted(scheduled, key=lambda item: (item[0], item[1]))]
    extra = int(anomalies)
    accepted_sensor_count = sample_count * machines + extra
    accepted_labels = sample_count * machines + extra
    expected = {
        "source": len(events),
        "accepted": accepted_sensor_count + accepted_labels + machines,
        "accepted_sensor": accepted_sensor_count, "accepted_label": accepted_labels,
        "accepted_heartbeat": machines, "duplicates": extra, "rejected": 6 * extra,
    }
    manifest = {
        "dataset_id": dataset_id, "parameters": parameters,
        "data_origin": "Generated synthetic measurements; no external or client data",
        "sensor_start": iso_time(start_time),
        "sensor_end": iso_time(start_time + (sample_count - 1) * step_seconds),
        "observation_end": iso_time(start_time + duration),
        "last_arrival_at": iso_time(max(item[0] for item in scheduled)),
        "sensor_count": accepted_sensor_count, "positive_labels": positive_count,
        "failure_schedule": {machine: [iso_time(time) for time in values] for machine, values in failures.items()},
        "injected_anomalies": injected, "expected_dispositions": expected,
        "records_sha256": content_hash("\n".join(canonical_json(event) if isinstance(event, dict) else event for event in events)),
    }
    return {"events": events, "manifest": manifest}

