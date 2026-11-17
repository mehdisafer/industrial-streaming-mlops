"""Bounded binary/software inventory and compressed-text provenance tests."""
from __future__ import annotations

import gzip
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.repository_archive_inventory import (  # noqa: E402
    TrackedBlob,
    git_blob_oid,
    inspect_tracked_archives,
    parse_git_tree,
)


def fixture(root: Path, name: str, payload: bytes) -> TrackedBlob:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return TrackedBlob(path=name, oid=git_blob_oid(payload), size=len(payload))


def test_valid_compressed_evidence_is_counted_and_sha_bound(tmp_path: Path) -> None:
    decompressed = b'{"sensor": 42}\n{"sensor": 43}\n'
    blob = fixture(tmp_path, "docs/evidence/native/input.jsonl.gz", gzip.compress(decompressed))
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "passed"
    assert result["compressed_text_count"] == 1
    assert result["compressed_total_bytes"] == blob.size
    assert result["compressed_text_evidence"][0]["git_blob"] == blob.oid
    assert result["compressed_text_evidence"][0]["decompressed_sha256"] == (
        hashlib.sha256(decompressed).hexdigest()
    )


def test_compressed_log_detects_secret_without_exposing_value(tmp_path: Path) -> None:
    token = "ghp_" + "A" * 36
    blob = fixture(
        tmp_path, "docs/evidence/native/worker.log.gz",
        gzip.compress(f"ordinary line\ncredential={token}\n".encode()),
    )
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["secret_pattern_findings"] == [{
        "path": blob.path, "line": 2, "rule": "github_classic_token"
    }]
    assert token not in str(result)


def test_corrupt_gzip_fails_without_being_marked_scanned(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/broken.log.gz", b"not gzip")
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["compressed_text_count"] == 0
    assert result["errors"]


def test_oversized_decompression_is_bounded(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/native/large.log.gz", gzip.compress(b"x" * 200))
    result = inspect_tracked_archives(tmp_path, [blob], max_decompressed_bytes=64)
    assert result["status"] == "failed"
    assert result["errors"][0]["reason"] == "decompression_limit_exceeded"


def test_modified_compressed_worktree_bytes_are_detected(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/native/data.csv.gz", gzip.compress(b"a,b\n1,2\n"))
    (tmp_path / blob.path).write_bytes(gzip.compress(b"a,b\n9,9\n"))
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["errors"][0]["reason"] == "worktree_bytes_differ_from_git_head"


def test_opaque_or_software_binary_is_not_silently_discarded(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/native/external.jar", b"binary")
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["compressed_text_count"] == 0
    assert result["tracked_software_or_opaque_archives"][0]["path"] == blob.path


def test_compressed_file_outside_evidence_is_not_whitelisted(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "src/industrial_mlops/unknown.log.gz", gzip.compress(b"hi"))
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["errors"][0]["reason"] == "unreviewed_compressed_path_or_mode"


def test_nontext_compressed_evidence_fails_scope(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/native/binary.log.gz", gzip.compress(b"\xff"))
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["compressed_text_count"] == 0
    assert result["errors"][0]["reason"] == "UnicodeDecodeError"


def test_compressed_file_with_unreviewed_extension_is_rejected(tmp_path: Path) -> None:
    blob = fixture(tmp_path, "docs/evidence/native/mystery.gz", gzip.compress(b"hello"))
    result = inspect_tracked_archives(tmp_path, [blob])
    assert result["status"] == "failed"
    assert result["errors"][0]["reason"] == "unreviewed_compressed_path_or_mode"


def test_git_ls_tree_parser_handles_padded_sizes() -> None:
    oid = "a" * 40
    entry = f"100644 blob {oid}       14\tdocs/evidence/a.log.gz\0".encode()
    assert parse_git_tree(entry) == [
        TrackedBlob("docs/evidence/a.log.gz", oid, 14)
    ]
