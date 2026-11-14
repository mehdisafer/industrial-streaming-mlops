"""Pure offset reconciliation; no runner imports, services or provider execution.

The compact backlog/fresh fixtures preserve restore06's batches 18..21 and its
producer offset receipts. That run reported 606 input rows for 303 source offsets.
The first local offset-verifier revision also accepted those offsets after
replacing the source description with FileStreamSource; the source-identity
regressions retain that observed counterexample. These tests do not relabel
restore06 as a successful run.
"""

import ast
import copy
from pathlib import Path

import pytest


RUNNER = Path(__file__).resolve().parents[1] / "scripts" / "qualify_restore.py"
DEFINITIONS = [node for node in ast.parse(RUNNER.read_text()).body
               if isinstance(node, ast.FunctionDef) and node.name == "verify_progress_ranges"]
assert len(DEFINITIONS) == 1
NAMESPACE = {}
exec(compile(ast.Module(body=DEFINITIONS, type_ignores=[]), str(RUNNER), "exec"), NAMESPACE)
VERIFY = NAMESPACE["verify_progress_ranges"]
TOPIC = "restore-backlog"
MISSING = object()


def batch(batch_id, start, end, reported):
    return {"batchId": batch_id, "numInputRows": reported, "sources": [{
        "description": f"KafkaV2[Subscribe[{TOPIC}]]",
        "startOffset": {TOPIC: dict(start)}, "endOffset": {TOPIC: dict(end)},
        "numInputRows": reported,
    }]}


@pytest.fixture
def example():
    start = {"0": 1162, "1": 1155, "2": 1154}
    middle = {"0": 1194, "1": 1188, "2": 1288}
    end = {"0": 1211, "1": 1205, "2": 1358}
    return {
        "progress": [batch(18, start, middle, 398), batch(19, middle, end, 208)],
        "topic": TOPIC, "start_offsets": start,
        "publication": {"topic": TOPIC, "partitions": 3, "acknowledged": 303,
                        "offsets": {"0": {"first": 1162, "last": 1210, "count": 49},
                                    "1": {"first": 1155, "last": 1204, "count": 50},
                                    "2": {"first": 1154, "last": 1357, "count": 204}}},
    }


def replace(case, path, value):
    current = case
    for key in path[:-1]:
        current = current[key]
    if value is MISSING:
        del current[path[-1]]
    else:
        current[path[-1]] = copy.deepcopy(value)


def rejected(case):
    result = VERIFY(**case)
    assert result["valid"] is False
    assert isinstance(result.get("reason"), str) and result["reason"]
    return result


def test_recorded_backlog_has_303_offsets_while_preserving_606_reported_rows(example):
    before = copy.deepcopy(example)
    result = VERIFY(**example)
    assert result["valid"] is True
    assert result["observed_offset_count"] == 303
    assert result["reported_input_rows"] == 606
    assert result["expected_start"] == {"0": 1162, "1": 1155, "2": 1154}
    assert result["expected_end"] == {"0": 1211, "1": 1205, "2": 1358}
    assert [row["batch_id"] for row in result["batches"]] == [18, 19]
    assert [row["offset_count"] for row in result["batches"]] == [199, 104]
    assert [row["reported_input_rows"] for row in result["batches"]] == [398, 208]
    assert example == before


def test_recorded_fresh_input_keeps_the_inactive_partition_without_a_receipt_entry():
    start = {"0": 1211, "1": 1205, "2": 1358}
    middle = {"0": 1211, "1": 1302, "2": 1460}
    end = {"0": 1211, "1": 1353, "2": 1513}
    publication = {"topic": TOPIC, "partitions": 3, "acknowledged": 303,
                   "offsets": {"1": {"first": 1205, "last": 1352, "count": 148},
                               "2": {"first": 1358, "last": 1512, "count": 155}}}
    result = VERIFY([batch(20, start, middle, 398), batch(21, middle, end, 208)],
                    TOPIC, start, publication)
    assert result["valid"] is True
    assert result["observed_offset_count"] == 303
    assert result["reported_input_rows"] == 606
    assert result["expected_end"] == end
    assert all(row["start"]["0"] == row["end"]["0"] == 1211 for row in result["batches"])


@pytest.mark.parametrize("reported", [0, 1, 303, 909])
def test_input_row_metric_is_never_normalized_or_used_as_an_offset_count(example, reported):
    for index, row in enumerate(example["progress"]):
        row["numInputRows"] = reported if index == 0 else 0
        row["sources"][0]["numInputRows"] = row["numInputRows"]
    result = VERIFY(**example)
    assert result["valid"] is True
    assert result["observed_offset_count"] == 303
    assert result["reported_input_rows"] == reported


def test_recorded_empty_resume_requires_observed_zero_offset_ranges():
    end = {"0": 1211, "1": 1353, "2": 1513}
    receipt = {"topic": TOPIC, "partitions": 3, "acknowledged": 0, "offsets": {}}
    result = VERIFY([batch(22, end, end, 0)], TOPIC, end, receipt)
    assert result["valid"] is True
    assert result["observed_offset_count"] == result["reported_input_rows"] == 0
    assert result["expected_start"] == result["expected_end"] == end
    assert len(result["batches"]) == 1
    assert VERIFY([], TOPIC, end, receipt)["valid"] is False
    advanced = {**end, "0": end["0"] + 1}
    assert VERIFY([batch(22, end, advanced, 0)], TOPIC, end, receipt)["valid"] is False


@pytest.mark.parametrize("progress", [None, [], {}, "missing"])
def test_absent_or_malformed_progress_is_not_evidence_of_completion(example, progress):
    example["progress"] = progress
    rejected(example)


@pytest.mark.parametrize("keep", [0, 1])
def test_truncated_progress_cannot_cover_the_full_publication(example, keep):
    example["progress"] = [example["progress"][keep]]
    rejected(example)


@pytest.mark.parametrize("delta", [-1, 1], ids=["overlap", "gap"])
def test_each_batch_must_begin_at_the_previous_complete_cursor(example, delta):
    example["progress"][1]["sources"][0]["startOffset"][TOPIC]["0"] += delta
    rejected(example)


@pytest.mark.parametrize("batch_id", [18, 20, 17], ids=["repeated", "skipped", "reordered"])
def test_spark_batch_ids_must_be_unique_and_consecutive(example, batch_id):
    example["progress"][1]["batchId"] = batch_id
    rejected(example)


@pytest.mark.parametrize("direction", ["regression", "overrun"])
def test_ranges_cannot_regress_or_read_beyond_the_acknowledged_publication(example, direction):
    source = example["progress"][1]["sources"][0]
    source["endOffset"][TOPIC]["2"] = (
        source["startOffset"][TOPIC]["2"] - 1 if direction == "regression" else 1359)
    rejected(example)


@pytest.mark.parametrize(("path", "value"), [
    pytest.param(("publication", "topic"), "another-topic", id="receipt-topic"),
    pytest.param(("progress", 0, "sources"), [], id="missing-source"),
    pytest.param(("progress", 0, "sources"), [{}, {}], id="extra-source"),
    pytest.param(("progress", 0, "sources", 0, "description"),
                 "FileStreamSource[explicit-unit-counterexample]", id="non-kafka-source"),
    pytest.param(("progress", 0, "sources", 0, "description"), MISSING, id="missing-source-description"),
    pytest.param(("progress", 0, "sources", 0, "description"),
                 "KafkaV2[Subscribe[another-topic]]", id="source-description-topic"),
    pytest.param(("progress", 0, "sources", 0, "startOffset"), None, id="null-start-offset"),
    pytest.param(("progress", 0, "sources", 0, "endOffset"), None, id="null-end-offset"),
    pytest.param(("progress", 0, "sources", 0, "startOffset"),
                 {"another-topic": {"0": 1162, "1": 1155, "2": 1154}}, id="foreign-start-topic"),
    pytest.param(("progress", 0, "sources", 0, "endOffset"),
                 {"another-topic": {"0": 1194, "1": 1188, "2": 1288}}, id="foreign-end-topic"),
    pytest.param(("progress", 0, "sources", 0, "startOffset", "another-topic"), {}, id="extra-topic"),
    pytest.param(("start_offsets", "2"), MISSING, id="missing-initial-partition"),
    pytest.param(("start_offsets", "3"), 0, id="extra-initial-partition"),
    pytest.param(("progress", 0, "sources", 0, "startOffset", TOPIC, "2"), MISSING, id="missing-source-partition"),
    pytest.param(("progress", 0, "sources", 0, "endOffset", TOPIC, "3"), 0, id="extra-source-partition"),
    pytest.param(("progress", 0, "sources", 0, "endOffset", TOPIC),
                 {0: 1194, "1": 1188, "2": 1288}, id="non-string-partition-key"),
    pytest.param(("publication", "offsets", "3"), {"first": 0, "last": 0, "count": 1}, id="extra-receipt-partition"),
    pytest.param(("publication", "offsets", "0", "first"), 1163, id="receipt-start-gap"),
    pytest.param(("publication", "offsets", "0", "first"), 1161, id="receipt-start-overlap"),
    pytest.param(("publication", "offsets", "0", "last"), 1209, id="receipt-range-count-mismatch"),
    pytest.param(("publication", "offsets", "0", "count"), 0, id="zero-count-receipt-entry"),
    pytest.param(("publication", "acknowledged"), 304, id="acknowledgements-do-not-sum"),
    pytest.param(("publication", "offsets"), {}, id="missing-acknowledged-ranges"),
    pytest.param(("progress", 1, "sources", 0, "endOffset", TOPIC, "2"), 1357, id="incomplete-final-range"),
])
def test_evidence_shape_identity_and_producer_receipt_must_reconcile(example, path, value):
    replace(example, path, value)
    rejected(example)


@pytest.mark.parametrize("path", [
    ("publication", "partitions"),
    ("publication", "acknowledged"),
    ("publication", "offsets", "0", "first"),
    ("publication", "offsets", "0", "last"),
    ("publication", "offsets", "0", "count"),
    ("start_offsets", "0"),
    ("progress", 0, "batchId"),
    ("progress", 0, "numInputRows"),
    ("progress", 0, "sources", 0, "startOffset", TOPIC, "0"),
    ("progress", 0, "sources", 0, "endOffset", TOPIC, "0"),
])
@pytest.mark.parametrize("value", [True, -1, 1.5, "1"])
def test_numeric_evidence_requires_nonnegative_integer_values_without_boolean_coercion(example, path, value):
    replace(example, path, value)
    rejected(example)


@pytest.mark.parametrize(("path", "value"), [
    (("publication",), None),
    (("publication", "offsets"), []),
    (("publication", "offsets", "0"), None),
    (("publication", "offsets", "0", "count"), MISSING),
    (("publication", "partitions"), 0),
    (("start_offsets",), []),
    (("progress", 0), None),
    (("progress", 0, "sources", 0), None),
    (("progress", 0, "numInputRows"), MISSING),
])
def test_malformed_receipts_return_an_explicit_invalid_result(example, path, value):
    replace(example, path, value)
    rejected(example)
