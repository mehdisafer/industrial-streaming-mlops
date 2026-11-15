"""Provider adapter semantics using pure Python and explicit in-memory table receipts.

These regression tests do not execute Spark, Delta, Kafka, a registry or Databricks.
"""

import copy
import builtins
import importlib.util
import json
from pathlib import Path
import sys
import tomllib
from types import SimpleNamespace

import pytest
import yaml

from industrial_mlops.config import PipelineConfig
from industrial_mlops.contracts import UNITS, canonical_json, iso_time, parse_time
from industrial_mlops.features import replay_with_report
from industrial_mlops.modeling import evaluate_scores, labeled_rows
from industrial_mlops.simulator import generate_dataset


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("provider_adapter_unit", ROOT / "databricks" / "job.py")
ADAPTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ADAPTER)
CONFIG = PipelineConfig()
CODE_SHA = "a" * 40
MODEL = {"model_uri": "models:/unit-provider-only/1", "model_name": "unit-provider-only",
         "model_version": "1", "model_run_id": "unit-run", "threshold": 0.5}
ORIGIN = parse_time("2026-01-01T00:00:00Z")


def sensor(event_id="s1", minute=0, machine="m1"):
    return {"schema_version": "1", "kind": "sensor", "event_id": event_id,
            "dataset_id": "unit", "source": "synthetic-unit", "site_id": "site", "machine_id": machine,
            "event_time": iso_time(ORIGIN + minute * 60), "ingested_at": iso_time(ORIGIN + minute * 60),
            "temperature_c": 70.0, "vibration_mm_s": 3.0, "pressure_bar": 6.0, "units": UNITS}


def label(anchor, event_id="l1", available=35, ingested=None, target=0, failure=None):
    return {key: anchor[key] for key in (
        "schema_version", "dataset_id", "source", "site_id", "machine_id", "event_time",
    )} | {"kind": "label", "event_id": event_id, "prediction_event_id": anchor["event_id"],
          "target": target, "failure_at": iso_time(ORIGIN + failure * 60) if failure is not None else None,
          "available_at": iso_time(ORIGIN + available * 60),
          "ingested_at": iso_time(ORIGIN + (available if ingested is None else ingested) * 60)}


def records(events, partitions=None):
    return [{"topic": "provider-unit", "partition": partitions[i] if partitions else 0,
             "offset": i, "value": canonical_json(event).encode() if isinstance(event, dict) else event}
            for i, event in enumerate(events)]


def feature(fid, eid, minute, available):
    return {"feature_id": fid, "event_id": eid, "dataset_id": "unit", "site_id": "site",
            "machine_id": "m1", "event_time": iso_time(ORIGIN + minute * 60),
            "available_at": iso_time(ORIGIN + available * 60)}


def emitted_payloads(alerts):
    return [{**json.loads(alert["payload"]), "alert_id": alert["alert_id"]} for alert in alerts]


@pytest.mark.parametrize("kind,reason", [
    ("orphan", "label_target_not_accepted"),
    ("identity", "label_target_identity_conflict"),
    ("second", "label_target_already_exists"),
])
def test_invalid_label_relationship_is_quarantined_and_matches_shared_replay(kind, reason):
    anchor = sensor()
    outcome = label(anchor)
    if kind == "orphan":
        events = [outcome]
    elif kind == "identity":
        events = [anchor, {**outcome, "machine_id": "m2"}]
    else:
        events = [anchor, outcome, {**outcome, "event_id": "l2"}]
    accepted, dispositions, payloads = ADAPTER.classify_records(records(events), CONFIG)
    _, reference = replay_with_report(payloads, CONFIG)
    assert dispositions[-1]["status"] == "rejected"
    assert dispositions[-1]["reason"] == reason
    assert len(accepted) == reference["counts"]["accepted"]
    assert sum(row["status"] == "rejected" for row in dispositions) == reference["counts"]["rejected"]
    assert reference["rejection_reasons"][reason] == 1


def test_rejected_label_does_not_claim_another_machines_partition():
    anchor = sensor()
    other = sensor("s2", 5, "m2")
    events = [anchor, {**label(anchor), "machine_id": "m2"}, other]
    accepted, dispositions, _ = ADAPTER.classify_records(records(events, [0, 0, 1]), CONFIG)
    assert [event["event_id"] for event in accepted] == ["s1", "s2"]
    assert dispositions[1]["reason"] == "label_target_identity_conflict"


def test_accepted_machine_cannot_switch_partitions():
    with pytest.raises(RuntimeError, match="changed Kafka partitions"):
        ADAPTER.classify_records(records([sensor(), sensor("s2", 5)], [0, 1]), CONFIG)


def test_label_for_rejected_late_sensor_is_rejected():
    events = [sensor("newer", 20), sensor("late", 0), label(sensor("late", 0))]
    _, dispositions, payloads = ADAPTER.classify_records(records(events), CONFIG)
    assert [row["reason"] for row in dispositions] == [None, "late_after_watermark", "label_target_not_accepted"]
    assert replay_with_report(payloads, CONFIG)[1]["counts"]["rejected"] == 2


def test_synthetic_replay_and_duplicate_dispositions_still_balance():
    dataset = generate_dataset(machines=2, sites=2, hours=2)
    events = [*dataset["events"], dataset["events"][0], b"\xff", None]
    accepted, dispositions, payloads = ADAPTER.classify_records(records(events), CONFIG)
    _, reference = replay_with_report(payloads, CONFIG)
    assert len(dispositions) == reference["counts"]["source"]
    assert len(accepted) == reference["counts"]["accepted"]
    for status, count_name in (("duplicate", "duplicates"), ("rejected", "rejected")):
        assert sum(row["status"] == status for row in dispositions) == reference["counts"][count_name]
    assert dispositions[-2]["reason"] == "invalid_utf8"


def test_tied_closure_time_uses_event_time_before_feature_identity():
    older = feature("z-older", "s1", 0, 10)
    newer = feature("a-newer", "s2", 5, 10)
    predictions, alerts = ADAPTER.prepare_prediction_outputs(
        [newer, older], {"z-older": 0.9, "a-newer": 0.9}, [], MODEL, CONFIG, CODE_SHA,
    )
    assert [p["feature_id"] for p in predictions] == ["z-older", "a-newer"]
    assert [p["alert_emitted"] for p in predictions] == [True, False]
    assert emitted_payloads(alerts)[0]["feature_id"] == "z-older"
    rows = [{"feature": f, "label": label(sensor(f["event_id"], 0 if f is older else 5))}
            for f in [older, newer]]
    assert evaluate_scores(rows, [0.9, 0.9], 0.5, CONFIG)["alerts"][0]["feature_id"] == "z-older"


def test_retry_recreates_alert_from_committed_prediction_without_rescoring():
    first = feature("f1", "s1", 0, 5)
    predictions, intended = ADAPTER.prepare_prediction_outputs([first], {"f1": 0.9}, [], MODEL, CONFIG, CODE_SHA)
    # Explicit in-memory analogue of a successful prediction write followed by
    # a failed alert write; this is not a Delta recovery integration result.
    unchanged = copy.deepcopy(predictions)
    new, repaired = ADAPTER.prepare_prediction_outputs([first], {}, predictions, MODEL, CONFIG, CODE_SHA)
    assert new == []
    assert repaired == intended
    assert predictions == unchanged


def test_prior_emission_seeds_cooldown_for_new_batch_and_is_never_rewritten():
    first = feature("f1", "s1", 10, 15)
    initial, first_alert = ADAPTER.prepare_prediction_outputs([first], {"f1": 0.9}, [], MODEL, CONFIG, CODE_SHA)
    later = feature("f2", "s2", 15, 20)
    retrograde = feature("f3", "s3", 5, 10)
    new, alerts = ADAPTER.prepare_prediction_outputs(
        [first, later, retrograde], {"f2": 0.9, "f3": 0.9}, initial, MODEL, CONFIG, CODE_SHA,
    )
    assert all(not p["alert_emitted"] for p in new)
    assert alerts == first_alert
    assert initial[0]["alert_emitted"] is True


def test_provider_accepts_no_legacy_or_mutated_committed_decision():
    first = feature("f1", "s1", 0, 5)
    prior, _ = ADAPTER.prepare_prediction_outputs([first], {"f1": 0.9}, [], MODEL, CONFIG, CODE_SHA)
    del prior[0]["alert_emitted"]
    with pytest.raises(ValueError, match="immutable alert decision"):
        ADAPTER.prepare_prediction_outputs([first], {}, prior, MODEL, CONFIG, CODE_SHA)


def test_delayed_label_does_not_invent_an_alert_for_a_suppressed_prediction():
    first, second = feature("f1", "s1", 0, 5), feature("f2", "s2", 5, 10)
    features = [first, second]
    scores = {"f1": 0.9, "f2": 0.9}
    _, alerts = ADAPTER.prepare_prediction_outputs(features, scores, [], MODEL, CONFIG, CODE_SHA)
    events = [label(sensor("s1", 0), available=60), label(sensor("s2", 5), "l2", available=35)]
    as_of = ORIGIN + 40 * 60
    naive_rows, _ = labeled_rows(features, events, as_of=as_of)
    assert evaluate_scores(naive_rows, [0.9], 0.5, CONFIG)["alerts"][0]["feature_id"] == "f2"
    observed = ADAPTER.evaluate_emitted_alerts(features, events, scores, emitted_payloads(alerts), 0.5, CONFIG, as_of=as_of)
    assert observed["emitted_alert_count"] == 1
    assert observed["pending_alert_labels"] == 1
    assert observed["mature_labels"] == 1
    assert observed["alert_count"] == observed["false_alerts"] == 0
    assert observed["alert_precision"] is None
    assert observed["alerts"] == []
    matured = ADAPTER.evaluate_emitted_alerts(features, events, scores, emitted_payloads(alerts), 0.5, CONFIG, as_of=ORIGIN + 61 * 60)
    assert matured["pending_alert_labels"] == 0
    assert matured["alert_count"] == matured["false_alerts"] == 1
    assert matured["alerts"][0]["feature_id"] == "f1"


def test_transport_delay_and_positive_lead_use_actual_alert_availability():
    first, second = feature("f1", "s1", 0, 5), feature("f2", "s2", 5, 10)
    scores = {"f1": 0.9, "f2": 0.9}
    _, alerts = ADAPTER.prepare_prediction_outputs([first, second], scores, [], MODEL, CONFIG, CODE_SHA)
    events = [label(sensor("s1", 0), available=20, ingested=60, target=1, failure=20),
              label(sensor("s2", 5), "l2", available=20, target=1, failure=20)]
    pending = ADAPTER.evaluate_emitted_alerts([first, second], events, scores, emitted_payloads(alerts), 0.5, CONFIG, as_of=ORIGIN + 30 * 60)
    assert pending["pending_alert_labels"] == 1
    assert pending["detected_failures"] == 0
    assert pending["useful_failure_recall"] == 0.0
    mature = ADAPTER.evaluate_emitted_alerts([first, second], events, scores, emitted_payloads(alerts), 0.5, CONFIG, as_of=ORIGIN + 60 * 60)
    assert mature["true_alerts"] == mature["observed_failures"] == mature["detected_failures"] == 1
    assert mature["useful_failure_recall"] == 1.0
    assert mature["lead_seconds_median"] == 900.0


def test_no_available_labels_remains_explicitly_pending():
    first = feature("f1", "s1", 0, 5)
    _, alerts = ADAPTER.prepare_prediction_outputs([first], {"f1": 0.9}, [], MODEL, CONFIG, CODE_SHA)
    report = ADAPTER.evaluate_emitted_alerts([first], [], {"f1": 0.9}, emitted_payloads(alerts), 0.5, CONFIG, as_of=ORIGIN + 10 * 60)
    assert report["status"] == "awaiting_labels"
    assert report["pending_labels"] == report["pending_alert_labels"] == 1
    assert report["alert_precision"] is report["false_alerts_per_machine_day"] is report["failure_recall"] is None


@pytest.mark.parametrize("rate_target,observed", [(1, 3), (1000, 1001)])
def test_kafka_partition_rounding_can_exceed_rate_target_below_hard_sink_cap(rate_target, observed):
    # Spark 3.5.9's proportional allocator gives each nonempty partition at
    # least one record: [10, 10, 10] with target 1 -> 3; [1_000_000, 1, 1]
    # with target 1000 -> 1001. These are source-contract counterexamples,
    # not a Kafka/Spark integration execution.
    receipt = ADAPTER.source_batch_limits(observed, rate_target, CONFIG.max_batch_rows)
    assert receipt == {"input_rows": observed, "max_offsets_per_trigger": rate_target,
                       "max_batch_rows": CONFIG.max_batch_rows, "rate_target_exceeded": True}


def test_partition_rounding_never_bypasses_the_hard_sink_cap():
    with pytest.raises(RuntimeError, match="hard sink row cap"):
        ADAPTER.source_batch_limits(CONFIG.max_batch_rows + 1, 1000, CONFIG.max_batch_rows)
    receipt = ADAPTER.source_batch_limits(0, 1000, CONFIG.max_batch_rows)
    assert receipt["input_rows"] == 0
    assert receipt["rate_target_exceeded"] is False


def test_rate_target_cannot_exceed_configured_sink_cap():
    with pytest.raises(ValueError, match="no greater than the hard sink row cap"):
        ADAPTER.source_batch_limits(0, 1000, 500)


@pytest.mark.parametrize("tracking_uri,registry_uri", [
    ("databricks", "databricks"),
    ("https://tracking.example.invalid", "databricks-uc"),
])
def test_unsupported_provider_layout_is_rejected_before_any_mlflow_import(monkeypatch, tracking_uri, registry_uri):
    original_import = builtins.__import__

    def import_guard(name, *args, **kwargs):
        if name == "mlflow" or name.startswith("mlflow."):
            raise AssertionError("Rejected provider layouts must not import or initialize MLflow")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_guard)
    arguments = SimpleNamespace(tracking_uri=tracking_uri, registry_uri=registry_uri,
                                model_name="unit.unit.model")
    with pytest.raises(ValueError, match="workspace registries and external tracking"):
        ADAPTER.load_model(arguments, CONFIG)


def test_native_uc_registry_layout_and_model_name_are_explicit():
    receipt = ADAPTER.registry_preflight("databricks", "databricks-uc", "unit.unit.model")
    assert receipt["model_name"] == "unit.unit.model"
    assert receipt["registry_uri"] == "databricks-uc"
    with pytest.raises(ValueError, match="catalog.schema.model"):
        ADAPTER.registry_preflight("databricks", "databricks-uc", "workspace_model")


def test_overlay_pins_are_the_reviewed_lock_and_job_configuration():
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    # The universal lock may contain multiple versions for distinct Python
    # targets. The runtime preflight separately checks this interpreter's pins.
    locked = {(p["name"], p["version"]) for p in lock["package"]}
    job = yaml.safe_load((ROOT / "resources" / "qualification.yml").read_text())
    libraries = job["resources"]["jobs"]["qualify_stream"]["tasks"][0]["libraries"]
    overlay = dict(library["pypi"]["package"].split("==") for library in libraries)
    assert overlay == ADAPTER.PROVIDER_PACKAGE_PINS
    assert all((name, version) in locked for name, version in overlay.items())
    assert "pyspark" not in overlay


def test_runtime_preflight_rejects_old_vendor_numpy_before_imports(monkeypatch):
    version = ADAPTER.importlib.metadata.version
    monkeypatch.setattr(ADAPTER.importlib.metadata, "version", lambda name: "1.23.5" if name == "numpy" else version(name))
    with pytest.raises(RuntimeError, match="numpy expected 2.3.5, observed 1.23.5"):
        ADAPTER.runtime_import_preflight()


def test_runtime_preflight_rejects_stale_loaded_module_despite_updated_metadata(monkeypatch):
    imports = ADAPTER.importlib
    local_import_double = SimpleNamespace(
        metadata=imports.metadata,
        import_module=lambda name, package=None: SimpleNamespace(__version__="1.23.5") if name == "numpy" else imports.import_module(name, package=package),
    )
    monkeypatch.setattr(ADAPTER, "importlib", local_import_double)
    with pytest.raises(RuntimeError, match="Loaded provider module mismatch: numpy"):
        ADAPTER.runtime_import_preflight()


def test_used_overlay_imports_and_numeric_abi_in_clean_local_environment(monkeypatch):
    monkeypatch.setenv("MLFLOW_DISABLE_TELEMETRY", "true")
    monkeypatch.setenv("DO_NOT_TRACK", "true")
    result = ADAPTER.runtime_import_preflight()
    assert result["status"] == "imports_checked"
    assert result["packages"] == ADAPTER.PROVIDER_PACKAGE_PINS
    assert result["loaded_versions"] == ADAPTER.PROVIDER_PACKAGE_PINS
    assert all(result["checks"].values())


def test_job_does_not_accept_local_spark_as_databricks(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    arguments = {"catalog": "test", "schema": "test", "table-prefix": "test", "output-root": "/Volumes/test/test/test/output",
                 "checkpoint-root": "/Volumes/test/test/test/checkpoint", "bootstrap": "kafka.example.invalid:9093",
                 "topic": "synthetic", "secret-scope": "test", "secret-key": "test", "model-name": "test.test.model",
                 "model-version": "1", "code-sha": CODE_SHA, "run-id": "1"}
    monkeypatch.setattr(sys, "argv", ["job.py", *[part for key, value in arguments.items() for part in ("--" + key, value)]])
    with pytest.raises(RuntimeError, match="real Databricks Runtime"):
        ADAPTER.main()


class FakeCheckpointFS:
    """Explicit in-memory ls/head/mkdirs/put double; no object-store/DBFS validation."""

    def __init__(self):
        self.parent = "/Volumes/unit/unit/unit/qualification"
        self.root = self.parent + "/checkpoint"
        self.directories = {self.parent}
        self.files = {}
        self.writes = []
        self.read_failure = None

    def ls(self, path):
        if self.read_failure == path:
            raise PermissionError("explicit fake filesystem permission failure")
        if path not in self.directories:
            raise FileNotFoundError(path)
        rows = []
        for directory in self.directories:
            parent, _, name = directory.rpartition("/")
            if parent == path:
                rows.append(SimpleNamespace(name=name + "/", size=0, path=directory + "/"))
        for file, content in self.files.items():
            parent, _, name = file.rpartition("/")
            if parent == path:
                rows.append(SimpleNamespace(name=name, size=len(content.encode()), path=file))
        return rows

    def head(self, path, max_bytes):
        if self.read_failure == path:
            raise PermissionError("explicit fake marker read failure")
        return self.files[path].encode()[:max_bytes].decode()

    def mkdirs(self, path):
        self.writes.append(("mkdirs", path))
        self.directories.add(path)
        return True

    def put(self, path, content, overwrite):
        assert overwrite is False
        if path in self.files:
            raise FileExistsError(path)
        self.writes.append(("put", path))
        self.files[path] = content
        return True


def test_remote_dbutils_is_rejected_without_initializing_auth_or_touching_files():
    from databricks.sdk.dbutils import RemoteDbUtils

    # Deliberately bypass construction: no Config, WorkspaceClient or API access.
    remote = object.__new__(RemoteDbUtils)
    with pytest.raises(RuntimeError, match="RemoteDbUtils fallback is unsupported"):
        ADAPTER.native_dbutils_preflight(remote)


@pytest.mark.parametrize("dbutils", [None, SimpleNamespace(fs=object())])
def test_missing_or_incomplete_dbutils_is_rejected(dbutils):
    with pytest.raises(RuntimeError, match="Databricks"):
        ADAPTER.native_dbutils_preflight(dbutils)


def test_native_interface_preflight_records_explicit_double_without_filesystem_calls():
    fs = FakeCheckpointFS()
    # This exercises control flow only, not the actual native Databricks object.
    receipt = ADAPTER.native_dbutils_preflight(SimpleNamespace(fs=fs))
    assert receipt["status"] == "native_interface_selected"
    assert receipt["implementation"] == "types.SimpleNamespace"
    assert fs.writes == []


def checkpoint_manifest(fs, prefix="qualified"):
    return {"code_sha": CODE_SHA, "checkpoint_root": fs.root, "catalog": "unit", "schema": "unit",
            "table_prefix": prefix, "config": CONFIG.to_dict(), "model": MODEL,
            "source_manifest": {"unit_source": "b" * 64}}


def test_fresh_checkpoint_is_bound_and_existing_deployment_matches_without_writes():
    fs = FakeCheckpointFS()
    manifest = checkpoint_manifest(fs)
    created = ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)
    assert created["status"] == "created"
    assert [call[0] for call in fs.writes] == ["mkdirs", "put"]
    fs.directories.add(fs.root + "/offsets")
    fs.writes.clear()
    matched = ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=True)
    assert matched["status"] == "matched"
    assert matched["manifest_sha256"] == created["manifest_sha256"]
    assert fs.writes == []


def test_existing_empty_checkpoint_can_be_bound_without_overwriting():
    fs = FakeCheckpointFS()
    fs.directories.add(fs.root)
    ADAPTER.bind_checkpoint(fs, fs.root, checkpoint_manifest(fs), namespace_exists=False)
    assert [call[0] for call in fs.writes] == ["put"]


def test_new_namespace_rejects_populated_unbound_checkpoint_before_any_write():
    fs = FakeCheckpointFS()
    fs.directories.update({fs.root, fs.root + "/offsets"})
    with pytest.raises(ValueError, match="no matching ownership marker"):
        ADAPTER.bind_checkpoint(fs, fs.root, checkpoint_manifest(fs), namespace_exists=False)
    assert fs.writes == []


def test_new_namespace_cannot_reuse_another_prefixs_bound_checkpoint():
    fs = FakeCheckpointFS()
    ADAPTER.bind_checkpoint(fs, fs.root, checkpoint_manifest(fs, "first"), namespace_exists=False)
    fs.writes.clear()
    with pytest.raises(ValueError, match="different or unreadable deployment"):
        ADAPTER.bind_checkpoint(fs, fs.root, checkpoint_manifest(fs, "second"), namespace_exists=False)
    assert fs.writes == []


def test_marker_only_retry_is_allowed_but_populated_checkpoint_with_lost_namespace_is_not():
    fs = FakeCheckpointFS()
    manifest = checkpoint_manifest(fs)
    ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)
    fs.writes.clear()
    assert ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)["status"] == "matched"
    fs.directories.add(fs.root + "/offsets")
    with pytest.raises(ValueError, match="cannot initialize a new Delta namespace"):
        ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)
    assert fs.writes == []


def test_existing_delta_deployment_cannot_adopt_a_missing_checkpoint():
    fs = FakeCheckpointFS()
    with pytest.raises(ValueError, match="cannot adopt a missing checkpoint"):
        ADAPTER.bind_checkpoint(fs, fs.root, checkpoint_manifest(fs), namespace_exists=True)
    assert fs.writes == []


@pytest.mark.parametrize("where", ["parent", "root", "marker"])
def test_checkpoint_read_permission_errors_never_mean_empty_or_missing(where):
    fs = FakeCheckpointFS()
    manifest = checkpoint_manifest(fs)
    ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)
    fs.writes.clear()
    fs.read_failure = {"parent": fs.parent, "root": fs.root,
                       "marker": fs.root + "/" + ADAPTER.CHECKPOINT_BINDING}[where]
    with pytest.raises(PermissionError, match="explicit fake"):
        ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=True)
    assert fs.writes == []


@pytest.mark.parametrize("root", ["/Volumes/unit/unit/unit", "s3://unit", "/Volumes/unit/unit/unit/../other",
                                  "s3://unit/checkpoint?credential=unit-fixture"])
def test_checkpoint_root_requires_a_dedicated_path_without_traversal_or_query(root):
    fs = FakeCheckpointFS()
    manifest = {**checkpoint_manifest(fs), "checkpoint_root": root}
    with pytest.raises(ValueError, match="dedicated child directory"):
        ADAPTER.bind_checkpoint(fs, root, manifest, namespace_exists=False)
    assert fs.writes == []


def test_checkpoint_marker_size_is_bounded_before_reads_or_writes():
    fs = FakeCheckpointFS()
    manifest = checkpoint_manifest(fs)
    ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=False)
    fs.files[fs.root + "/" + ADAPTER.CHECKPOINT_BINDING] = "x" * (ADAPTER.MAX_CHECKPOINT_BINDING_BYTES + 1)
    fs.writes.clear()
    with pytest.raises(ValueError, match="invalid size"):
        ADAPTER.bind_checkpoint(fs, fs.root, manifest, namespace_exists=True)
    assert fs.writes == []
