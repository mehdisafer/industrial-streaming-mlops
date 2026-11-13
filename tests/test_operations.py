import json
import sqlite3

import pytest

from industrial_mlops.operations import backup_state, restore_state
from industrial_mlops.store import Store


def test_restore_requires_new_target_and_detects_changed_bytes(tmp_path):
    state = tmp_path / "state"
    with Store(state / "pipeline" / "monitor.db") as store:
        assert store.verify()["valid"]
    backup = tmp_path / "backup"
    with pytest.raises(ValueError):
        backup_state(state, backup, quiesced=False)
    manifest = backup_state(state, backup, quiesced=True)
    assert manifest["files"]
    target = tmp_path / "restored"
    restore_state(backup, target)
    with Store(target / "pipeline" / "monitor.db", readonly=True) as store:
        assert store.verify()["valid"]
    with pytest.raises(ValueError, match="already exists"):
        restore_state(backup, target)
    (backup / "data" / "pipeline" / "monitor.db").write_bytes(b"corruption")
    with pytest.raises(ValueError, match="integrity mismatch"):
        restore_state(backup, tmp_path / "bad-restore")
    assert not (tmp_path / "bad-restore").exists()


def test_restore_rejects_path_traversal(tmp_path):
    backup = tmp_path / "backup"
    backup.mkdir()
    (backup / "manifest.json").write_text(json.dumps({
        "schema_version": "1", "files": {"../../escape": "abc"}}))
    with pytest.raises(ValueError, match="unsafe"):
        restore_state(backup, tmp_path / "restored")


def test_restore_rejects_symlink_parent(tmp_path):
    import hashlib
    backup = tmp_path / "backup"
    (backup / "data").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"example")
    (backup / "data" / "escape").symlink_to(outside, target_is_directory=True)
    (backup / "manifest.json").write_text(json.dumps({
        "schema_version": "1", "files": {
            "escape/secret": hashlib.sha256(b"example").hexdigest()}}))
    with pytest.raises(ValueError, match="unsafe"):
        restore_state(backup, tmp_path / "restored")


def test_binary_poison_preserves_original_kafka_bytes(tmp_path):
    import base64
    import hashlib
    payload = b"\xff\xfe\x00poison"
    with Store(tmp_path / "binary.db") as store:
        store.process_batch([{
            "topic": "binary", "partition": 0, "offset": 0, "value": "replacement",
            "value_base64": base64.b64encode(payload).decode(),
        }], "binary", 0)
        row = store.db.execute("SELECT * FROM raw_records").fetchone()
        assert row["disposition"] == "rejected"
        assert row["payload_sha256"] == hashlib.sha256(payload).hexdigest()
        assert base64.b64decode(row["payload_base64"]) == payload
        assert store.verify()["valid"]


def test_kafka_tombstone_is_distinct_from_empty_payload(tmp_path):
    with Store(tmp_path / "null.db") as store:
        store.process_batch([{"topic": "t", "partition": 0, "offset": 0, "value": None}], "q", 0)
        row = store.db.execute("SELECT * FROM raw_records").fetchone()
        assert row["payload_is_null"] == 1
        assert "kafka_tombstone" in row["reason"]
        with pytest.raises(ValueError, match="coordinate content changed"):
            store.process_batch([{"topic": "t", "partition": 0, "offset": 0, "value": ""}], "q", 1)


def test_spark_mime_wrapped_base64_preserves_bytes(tmp_path):
    import base64
    from industrial_mlops.store import source_bytes
    payload = b'{"kind":"sensor","padding":"' + b"x" * 500 + b'"}'
    wrapped = base64.encodebytes(payload).decode()
    assert "\n" in wrapped
    assert source_bytes({"value": "", "value_base64": wrapped}) == payload


def test_store_config_change_requires_migration(tmp_path):
    from industrial_mlops.config import PipelineConfig
    path = tmp_path / "db"
    with Store(path):
        pass
    with pytest.raises(ValueError, match="incompatible"):
        Store(path, PipelineConfig(window_seconds=900))
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
