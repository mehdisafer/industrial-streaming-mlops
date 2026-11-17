"""Audit tracked software binaries and bounded compressed text evidence.

Unlike a filename-only scan, this accepts documented .gz evidence without
pretending those files do not exist. No network calls or artifact rewrites.
This scans the checked-out tree, not unreachable history or external images.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
from typing import Iterable

from scripts.security_inventory import RULES

SOFTWARE_EXTENSIONS = (
    ".jar", ".whl", ".so", ".dll", ".exe", ".zip", ".tar",
    ".tar.gz", ".tgz", ".7z", ".rar", ".msi", ".dmg", ".class", ".pyd",
)
EVIDENCE_EXTENSIONS = (".jsonl.gz", ".log.gz", ".csv.gz")
MAX_DECOMPRESSED_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class TrackedBlob:
    path: str
    oid: str
    size: int
    mode: str = "100644"


def tracked_blobs(root: Path) -> list[TrackedBlob]:
    """Read exact Git HEAD blob records, including original Git object IDs."""
    listing = subprocess.check_output(
        ["git", "-C", str(root), "ls-tree", "-r", "-l", "-z", "HEAD"],
        stderr=subprocess.PIPE,
    )
    result: list[TrackedBlob] = []
    for record in listing.split(b"\0"):
        if not record:
            continue
        metadata, raw_path = record.split(b"\t", 1)
        mode, kind, oid, size = metadata.split(b" ")
        if kind != b"blob":
            continue
        result.append(
            TrackedBlob(raw_path.decode("utf-8"), oid.decode("ascii"),
                        int(size), mode.decode("ascii"))
        )
    return result


def git_blob_oid(payload: bytes) -> str:
    """The SHA-1 Git object identifier of the actual checked-out blob bytes."""
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload).hexdigest()


def inspect_tracked_archives(
    root: Path, blobs: Iterable[TrackedBlob],
    *, max_decompressed_bytes: int = MAX_DECOMPRESSED_BYTES,
) -> dict:
    software: list[dict] = []
    compressed: list[dict] = []
    findings: list[dict] = []
    errors: list[dict] = []
    for entry in blobs:
        path = entry.path
        lower = path.lower()
        is_software = lower.endswith(SOFTWARE_EXTENSIONS)
        is_gzip = lower.endswith(".gz")
        if not is_software and not is_gzip:
            continue
        if is_software:
            software.append({"path": path, "git_blob": entry.oid, "bytes": entry.size})
            errors.append({"path": path, "reason": "tracked_software_or_opaque_archive"})
            continue

        parts = PurePosixPath(path).parts
        if (
            not path.startswith("docs/evidence/")
            or not lower.endswith(EVIDENCE_EXTENSIONS)
            or any(part in (".", "..") for part in parts)
            or entry.mode != "100644"
        ):
            errors.append({"path": path, "reason": "unreviewed_compressed_path_or_mode"})
            continue

        try:
            file_path = root.joinpath(*parts)
            if file_path.is_symlink():
                raise ValueError("symlink_not_allowed")
            compressed_data = file_path.read_bytes()
            if entry.size != len(compressed_data) or git_blob_oid(compressed_data) != entry.oid:
                raise ValueError("worktree_bytes_differ_from_git_head")
            # Read at most cap + 1, so a compressed bomb does not expand unchecked.
            with gzip.open(file_path, "rb") as stream:
                decoded_bytes = stream.read(max_decompressed_bytes + 1)
            if len(decoded_bytes) > max_decompressed_bytes:
                raise ValueError("decompression_limit_exceeded")
            decoded = decoded_bytes.decode("utf-8")
        except (OSError, EOFError, UnicodeError, ValueError) as exc:
            # Preserve reason category, never raw compressed data or secret values.
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            errors.append({"path": path, "reason": reason})
            continue

        for lineno, line in enumerate(decoded.splitlines(), start=1):
            for rule_name, rule in RULES.items():
                if rule.search(line):
                    findings.append({"path": path, "line": lineno, "rule": rule_name})
        compressed.append({
            "path": path,
            "git_blob": entry.oid,
            "compressed_bytes": entry.size,
            "decompressed_bytes": len(decoded_bytes),
            "decompressed_sha256": hashlib.sha256(decoded_bytes).hexdigest(),
        })

    return {
        "tracked_software_or_opaque_archives": software,
        "compressed_text_evidence": compressed,
        "compressed_text_count": len(compressed),
        "compressed_total_bytes": sum(x["compressed_bytes"] for x in compressed),
        "secret_pattern_findings": findings,
        "errors": errors,
        "status": "passed" if not errors and not findings else "failed",
        "limits": [
            "The filename whitelist is not a third-party rights assessment.",
            "Only Git HEAD checked-out .gz evidence is decompressed and scanned.",
            "Only configured secret regex patterns are checked; no entropy/hosted scan.",
            "Git object IDs bind checked-out compressed bytes; expanded SHA-256 is also recorded.",
            "No unreachable Git history, external Docker layers, JVM downloads or installed wheels are inspected.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = {
        "schema_version": 1,
        "source_commit": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "tree_scope": "tracked Git HEAD blobs, with bounded current compressed content",
        **inspect_tracked_archives(root, tracked_blobs(root)),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({
        "status": report["status"],
        "compressed_text_count": report["compressed_text_count"],
        "binary_archive_count": len(report["tracked_software_or_opaque_archives"]),
        "secret_pattern_findings": len(report["secret_pattern_findings"]),
        "errors": len(report["errors"]),
    }, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
