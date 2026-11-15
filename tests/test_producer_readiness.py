"""Topic-readiness controls with strict client doubles; no broker is contacted."""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import kafka
from kafka import KafkaAdminClient
from kafka.errors import (
    InvalidTopicError, KafkaTimeoutError, LeaderNotAvailableError,
    RequestTimedOutError, TopicAlreadyExistsError, TopicAuthorizationFailedError,
    UnknownError, UnknownTopicOrPartitionError,
)
from kafka.producer.record_accumulator import RecordAccumulator
import pytest

from industrial_mlops import streaming


TOPIC = "readiness-unit"


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def perf_counter(self):
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        self.now += seconds


def metadata(*, count=3, code=0, leader=1):
    return [{"topic": TOPIC, "error_code": code, "partitions": [
        {"partition": part, "error_code": 0, "leader": leader,
         "replicas": [1], "isr": [1]}
        for part in range(count)
    ]}]


class AdminDouble:
    def __init__(self, responses, clock, *, create_error=None, describe_seconds=0):
        self.responses = responses
        self.clock = clock
        self.create_error = create_error
        self.describe_seconds = describe_seconds
        self.describe_calls = 0
        self.closed = False
        self.created_topics = []

    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        assert not self.closed
        assert validate_only is False
        self.created_topics = new_topics
        assert len(new_topics) == 1
        topic = new_topics[0]
        assert topic.name == TOPIC and topic.num_partitions == 3
        assert topic.replication_factor == 1
        assert topic.topic_configs == {
            "retention.ms": str(7 * 86400000), "max.message.bytes": "1048576",
        }
        if self.create_error is not None:
            raise self.create_error

    def describe_topics(self, topics=None):
        assert not self.closed and topics == [TOPIC]
        response = self.responses[min(self.describe_calls, len(self.responses) - 1)]
        self.describe_calls += 1
        self.clock.now += self.describe_seconds
        if isinstance(response, BaseException):
            raise response
        return response

    def close(self):
        assert not self.closed
        self.closed = True


class ProducerDouble:
    def __init__(self):
        self.sent = []
        self.flushed = False
        self.closed = False

    def send(self, topic, *, key, value):
        assert not self.closed and topic == TOPIC
        self.sent.append((key, value))
        return SimpleNamespace(get=lambda timeout: SimpleNamespace(partition=0, offset=0))

    def flush(self, timeout):
        assert timeout > 0 and not self.closed
        self.flushed = True

    def close(self, timeout):
        assert timeout > 0 and not self.closed
        self.closed = True


def install(monkeypatch, tmp_path, responses, *, ready_after=None, create_error=None,
            describe_seconds=0):
    clock = Clock()
    admin = AdminDouble(responses, clock, create_error=create_error,
                        describe_seconds=describe_seconds)
    producer = ProducerDouble()
    construction = []

    def create_admin(*, bootstrap_servers, request_timeout_ms):
        assert bootstrap_servers == "unit-broker.invalid:19092"
        assert 1000 <= request_timeout_ms <= 120000
        return admin

    def create_producer(*, bootstrap_servers, acks, enable_idempotence, max_block_ms,
                        request_timeout_ms, delivery_timeout_ms,
                        max_in_flight_requests_per_connection, max_request_size, linger_ms):
        # A failure path must never construct a producer, let alone send a record.
        assert ready_after is not None, "producer constructed without topic readiness"
        assert admin.closed and admin.describe_calls == ready_after
        assert bootstrap_servers == "unit-broker.invalid:19092"
        assert acks == "all" and enable_idempotence
        assert max_in_flight_requests_per_connection == 1
        assert max_request_size == 1048576 and linger_ms == 5
        assert max_block_ms == delivery_timeout_ms
        assert 0 < request_timeout_ms <= 30000
        assert delivery_timeout_ms >= request_timeout_ms + linger_ms
        producer.timeout_config = {
            "request_timeout_ms": request_timeout_ms,
            "delivery_timeout_ms": delivery_timeout_ms, "linger_ms": linger_ms,
        }
        construction.append(admin.describe_calls)
        return producer

    monkeypatch.setattr(kafka, "KafkaAdminClient", create_admin)
    monkeypatch.setattr(kafka, "KafkaProducer", create_producer)
    monkeypatch.setattr(streaming, "time", clock)
    path = tmp_path / "input.jsonl"
    path.write_text('{"dataset_id":"unit","site_id":"s","machine_id":"m"}\n')
    return admin, producer, clock, construction, path


def call_produce(path, *, timeout=2):
    return streaming.produce(str(path), "unit-broker.invalid:19092", TOPIC,
                             partitions=3, timeout_seconds=timeout)


def test_admin_double_matches_locked_client_method_signatures():
    for name in ("create_topics", "describe_topics", "close"):
        assert inspect.signature(getattr(AdminDouble, name)) == inspect.signature(
            getattr(KafkaAdminClient, name))


@pytest.mark.parametrize("timeout,request_ms", [(1, 995), (30, 29995), (31, 30000),
                                               (60, 30000), (120, 30000)])
def test_producer_timeout_bounds_satisfy_locked_sdk(monkeypatch, tmp_path, timeout, request_ms):
    admin, producer, _, _, path = install(monkeypatch, tmp_path, [metadata()], ready_after=1)
    result = call_produce(path, timeout=timeout)
    assert result["acknowledged"] == 1 and admin.closed and producer.closed
    assert producer.timeout_config["request_timeout_ms"] == request_ms
    assert producer.timeout_config["delivery_timeout_ms"] == timeout * 1000
    # The installed SDK enforces this invariant without needing a Kafka connection.
    accumulator = RecordAccumulator(**producer.timeout_config)
    assert accumulator.config["delivery_timeout_ms"] == timeout * 1000


def test_transient_missing_metadata_then_ready(monkeypatch, tmp_path):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata(count=0, code=3), metadata()], ready_after=2)
    result = call_produce(path)
    assert result["acknowledged"] == 1
    assert result["topic_readiness"]["attempts"] == 2
    assert result["topic_readiness"]["partition_ids"] == [0, 1, 2]
    assert result["topic_readiness"]["leaders"] == {"0": 1, "1": 1, "2": 1}
    assert result["topic_readiness"]["retry_reasons"] == {"UnknownTopicOrPartitionError": 1}
    assert result["topic_readiness"]["elapsed_seconds"] > 0
    assert construction == [2] and len(producer.sent) == 1
    assert admin.closed and producer.closed and producer.flushed and clock.sleeps


@pytest.mark.parametrize("transient", [[], metadata(count=0), metadata(leader=-1),
                                      UnknownTopicOrPartitionError("unit transient")])
def test_other_missing_metadata_states_wait_before_producer(monkeypatch, tmp_path, transient):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [transient, metadata()], ready_after=2)
    result = call_produce(path)
    assert result["topic_readiness"]["attempts"] == 2
    assert sum(result["topic_readiness"]["retry_reasons"].values()) == 1
    assert construction == [2] and clock.sleeps and producer.closed and admin.closed


def test_partition_leader_error_is_retried(monkeypatch, tmp_path):
    pending = metadata()
    pending[0]["partitions"][1].update(error_code=5, leader=-1)
    admin, producer, _, _, path = install(
        monkeypatch, tmp_path, [pending, metadata()], ready_after=2)
    result = call_produce(path)
    assert result["topic_readiness"]["retry_reasons"] == {"LeaderNotAvailableError": 1}
    assert admin.closed and producer.closed


def test_readiness_timeout_is_bounded_and_never_constructs_producer(monkeypatch, tmp_path):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata(count=0, code=3)])
    with pytest.raises(KafkaTimeoutError, match="metadata"):
        call_produce(path, timeout=1)
    assert clock.now == pytest.approx(101.0)
    assert 2 <= admin.describe_calls <= 12
    assert admin.closed and not construction and not producer.sent


def test_ready_response_after_deadline_is_not_allowed_to_publish(monkeypatch, tmp_path):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata()], describe_seconds=2)
    with pytest.raises(KafkaTimeoutError, match="metadata"):
        call_produce(path, timeout=1)
    assert admin.describe_calls == 1 and not clock.sleeps
    assert admin.closed and not construction and not producer.sent


@pytest.mark.parametrize("create_error", [None, TopicAlreadyExistsError("unit existing")])
@pytest.mark.parametrize("leader", [1, -1])
def test_real_valid_wrong_partition_count_is_rejected_immediately(
        monkeypatch, tmp_path, create_error, leader):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata(count=2, leader=leader)], create_error=create_error)
    with pytest.raises(ValueError, match="partition"):
        call_produce(path)
    assert admin.describe_calls == 1 and not clock.sleeps
    assert admin.closed and not construction and not producer.sent


@pytest.mark.parametrize("partition_ids", [[0, 1, 1], [1, 2, 3]])
def test_exact_partition_ids_are_required(monkeypatch, tmp_path, partition_ids):
    wrong = metadata()
    for row, partition in zip(wrong[0]["partitions"], partition_ids):
        row["partition"] = partition
    admin, _, clock, construction, path = install(monkeypatch, tmp_path, [wrong])
    with pytest.raises(ValueError, match="partition"):
        call_produce(path)
    assert admin.describe_calls == 1 and admin.closed and not clock.sleeps and not construction


@pytest.mark.parametrize("code,error", [(29, TopicAuthorizationFailedError), (17, InvalidTopicError),
                                       (7, RequestTimedOutError), (99999, UnknownError)])
def test_nonmetadata_or_permanent_topic_error_fails_without_retry(monkeypatch, tmp_path, code, error):
    admin, _, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata(count=0, code=code)])
    with pytest.raises(error) as failure:
        call_produce(path)
    assert failure.value.errno == code
    assert admin.describe_calls == 1 and admin.closed and not clock.sleeps and not construction


def test_permanent_partition_error_is_not_hidden_by_a_transient_partition(monkeypatch, tmp_path):
    response = metadata()
    response[0]["partitions"][0].update(error_code=5, leader=-1)
    response[0]["partitions"][1].update(error_code=29, leader=-1)
    admin, _, clock, construction, path = install(monkeypatch, tmp_path, [response])
    with pytest.raises(TopicAuthorizationFailedError):
        call_produce(path)
    assert admin.describe_calls == 1 and admin.closed and not clock.sleeps and not construction


@pytest.mark.parametrize("error", [TopicAuthorizationFailedError("unit denied"),
                                   KafkaTimeoutError("unit transport timeout")])
def test_describe_exception_fails_closed_and_closes_admin(monkeypatch, tmp_path, error):
    admin, _, clock, construction, path = install(monkeypatch, tmp_path, [error])
    with pytest.raises(type(error)):
        call_produce(path)
    assert admin.describe_calls == 1 and admin.closed and not clock.sleeps and not construction


def test_create_authorization_error_is_not_retried_or_published(monkeypatch, tmp_path):
    admin, _, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata()], create_error=TopicAuthorizationFailedError("unit denied"))
    with pytest.raises(TopicAuthorizationFailedError):
        call_produce(path)
    assert admin.describe_calls == 0 and admin.closed and not clock.sleeps and not construction


def test_existing_ready_topic_reuses_configuration_and_accepts_broker_zero(monkeypatch, tmp_path):
    admin, producer, clock, construction, path = install(
        monkeypatch, tmp_path, [metadata(leader=0)], ready_after=1,
        create_error=TopicAlreadyExistsError("unit existing"))
    result = call_produce(path)
    assert result["topic_readiness"]["attempts"] == 1
    assert result["topic_readiness"]["leaders"] == {"0": 0, "1": 0, "2": 0}
    assert result["topic_readiness"]["retry_reasons"] == {}
    assert construction == [1] and admin.closed and producer.closed and not clock.sleeps


def test_retriable_leader_exception_can_recover(monkeypatch, tmp_path):
    admin, producer, _, construction, path = install(
        monkeypatch, tmp_path, [LeaderNotAvailableError("unit leader election"), metadata()],
        ready_after=2)
    result = call_produce(path)
    assert result["topic_readiness"]["retry_reasons"] == {"LeaderNotAvailableError": 1}
    assert construction == [2] and admin.closed and producer.closed
