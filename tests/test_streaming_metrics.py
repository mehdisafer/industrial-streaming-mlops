"""Source-lag unit checks with strict Kafka API doubles; no broker is contacted."""

from __future__ import annotations

import inspect
import json

import kafka
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable
import pytest

from industrial_mlops.store import Store
from industrial_mlops.streaming import source_lag


class ConsumerUnitDouble:
    """Match the offset-method signatures in the locked kafka-python 2.3.2 client.

    These methods deliberately do not accept arbitrary keywords: an unsupported
    timeout_ms argument must fail the observed-lag regression, not be swallowed
    by a permissive mock.
    """

    def __init__(self, *, starts=None, ends=None, metadata=None, failure=None):
        self.starts = starts or {}
        self.ends = ends or {}
        self.metadata = metadata
        self.failure = failure
        self.calls = []
        self.closed = False

    def partitions_for_topic(self, topic):
        assert topic == "telemetry-unit"
        self.calls.append(("metadata", topic))
        if self.failure == "metadata":
            raise KafkaTimeoutError("unit metadata timeout")
        return self.metadata

    def end_offsets(self, partitions):
        self.calls.append(("end_offsets", list(partitions)))
        if self.failure == "end_offsets":
            raise KafkaTimeoutError("unit end-offset timeout")
        return {partition: self.ends[partition.partition] for partition in partitions}

    def beginning_offsets(self, partitions):
        self.calls.append(("beginning_offsets", list(partitions)))
        if self.failure == "beginning_offsets":
            raise KafkaTimeoutError("unit beginning-offset timeout")
        return {partition: self.starts[partition.partition] for partition in partitions}

    def close(self):
        assert not self.closed
        self.closed = True


def install_consumer(monkeypatch, consumer, *, constructor_failure=False):
    def create_consumer(
        *, bootstrap_servers, group_id, request_timeout_ms, session_timeout_ms,
        api_version_auto_timeout_ms,
    ):
        assert bootstrap_servers == "unit-broker.invalid:19092"
        # Metrics must not join or commit a consumer group, and requests are bounded.
        assert group_id is None
        assert 0 < session_timeout_ms < request_timeout_ms <= 10000
        assert 0 < api_version_auto_timeout_ms <= request_timeout_ms
        if constructor_failure:
            raise NoBrokersAvailable("unit constructor failure")
        return consumer

    monkeypatch.setattr(kafka, "KafkaConsumer", create_consumer)


def source_record(partition, offset, value="unit-invalid-json", topic="telemetry-unit"):
    return {"topic": topic, "partition": partition, "offset": offset, "value": value}


def test_offset_double_matches_installed_client_api():
    for name in ("end_offsets", "beginning_offsets"):
        actual = inspect.signature(getattr(KafkaConsumer, name))
        double = inspect.signature(getattr(ConsumerUnitDouble, name))
        assert actual == double
        assert tuple(actual.parameters) == ("self", "partitions")


def test_source_lag_uses_all_committed_coordinates_and_isolates_topics(tmp_path, monkeypatch):
    consumer = ConsumerUnitDouble(
        starts={0: 10, 2: 100, 5: 7}, ends={0: 14, 2: 104, 5: 7}, metadata={5, 0, 2},
    )
    install_consumer(monkeypatch, consumer)
    sensor = json.dumps({
        "schema_version": "1", "kind": "sensor", "event_id": "unit-sensor-1",
        "dataset_id": "unit-dataset", "source": "unit-sensor", "site_id": "site-1",
        "machine_id": "machine-1", "event_time": "2026-01-01T00:00:00Z",
        "ingested_at": "2026-01-01T00:00:01Z", "temperature_c": 55.0,
        "vibration_mm_s": 1.0, "pressure_bar": 7.0,
        "units": {"temperature_c": "degC", "vibration_mm_s": "mm/s", "pressure_bar": "bar"},
    })
    with Store(tmp_path / "lag.db") as store:
        store.process_batch([
            source_record(0, 10, sensor),
            source_record(0, 11, sensor),  # A duplicate is still a consumed Kafka coordinate.
            source_record(0, 12),  # Quarantine also advances source reconciliation.
            source_record(2, 999, topic="another-topic"),
        ], "unit-lag", 0)
        before = store.counts()
        assert before["raw"] == 4
        assert before["accepted"] == 1
        assert before["duplicate"] == 1
        assert before["rejected"] == 2

        report = source_lag(store, "unit-broker.invalid:19092", "telemetry-unit")

        assert report == {
            "status": "observed", "topic": "telemetry-unit", "offsets_behind": 5,
            "partitions": [
                {"partition": 0, "retained_start": 10, "broker_end": 14,
                 "sink_next": 13, "behind": 1},
                {"partition": 2, "retained_start": 100, "broker_end": 104,
                 "sink_next": 100, "behind": 4},
                {"partition": 5, "retained_start": 7, "broker_end": 7,
                 "sink_next": 7, "behind": 0},
            ],
        }
        assert store.counts() == before
        assert store.verify()["valid"]
    expected_partitions = [TopicPartition("telemetry-unit", part) for part in (0, 2, 5)]
    assert consumer.calls == [
        ("metadata", "telemetry-unit"),
        ("end_offsets", expected_partitions),
        ("beginning_offsets", expected_partitions),
    ]
    assert consumer.closed


def test_source_lag_zero_means_raw_source_drained_even_when_records_rejected(tmp_path, monkeypatch):
    consumer = ConsumerUnitDouble(
        starts={0: 10, 2: 100}, ends={0: 13, 2: 102}, metadata={0, 2},
    )
    install_consumer(monkeypatch, consumer)
    with Store(tmp_path / "drained.db") as store:
        store.process_batch([
            source_record(partition, offset)
            for partition, offsets in ((0, (10, 11, 12)), (2, (100, 101)))
            for offset in offsets
        ], "unit-drained", 0)
        report = source_lag(store, "unit-broker.invalid:19092", "telemetry-unit")
        assert store.counts()["raw"] == store.counts()["rejected"] == 5
        assert store.counts()["predictions"] == 0
        assert report["status"] == "observed"
        assert report["offsets_behind"] == 0
        assert all(row["sink_next"] == row["broker_end"] for row in report["partitions"])
    assert consumer.closed


@pytest.mark.parametrize("metadata", [None, set()])
def test_source_lag_missing_metadata_is_unavailable_and_closes_client(tmp_path, monkeypatch, metadata):
    consumer = ConsumerUnitDouble(metadata=metadata)
    install_consumer(monkeypatch, consumer)
    with Store(tmp_path / "missing.db") as store:
        assert source_lag(store, "unit-broker.invalid:19092", "telemetry-unit") == {
            "status": "unavailable", "reason": "topic_metadata_unavailable",
        }
        assert store.counts()["raw"] == 0
    assert consumer.calls == [("metadata", "telemetry-unit")]
    assert consumer.closed


@pytest.mark.parametrize("failure", ["metadata", "end_offsets", "beginning_offsets"])
def test_source_lag_timeout_reports_unavailable_and_closes_client(tmp_path, monkeypatch, failure):
    consumer = ConsumerUnitDouble(starts={0: 0}, ends={0: 3}, metadata={0}, failure=failure)
    install_consumer(monkeypatch, consumer)
    with Store(tmp_path / "timeout.db") as store:
        before = store.counts()
        report = source_lag(store, "unit-broker.invalid:19092", "telemetry-unit")
        assert report == {"status": "unavailable", "exception": "KafkaTimeoutError"}
        assert store.counts() == before
    assert consumer.closed


def test_source_lag_unavailable_broker_at_construction_is_reported(tmp_path, monkeypatch):
    consumer = ConsumerUnitDouble()
    install_consumer(monkeypatch, consumer, constructor_failure=True)
    with Store(tmp_path / "unavailable.db") as store:
        assert source_lag(store, "unit-broker.invalid:19092", "telemetry-unit") == {
            "status": "unavailable", "exception": "NoBrokersAvailable",
        }
    assert not consumer.calls
    assert not consumer.closed
