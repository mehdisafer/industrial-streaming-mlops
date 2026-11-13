"""Bounded evidence collection and explicit stopped-state backup/restore."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import time
from pathlib import Path


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def source_manifest(root: str | Path = ".") -> dict:
    root = Path(root)
    files = []
    for name in ("src", "schemas", "config"):
        base = root / name
        if base.exists():
            files += [p for p in base.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    files += [root / name for name in ("pyproject.toml", "uv.lock") if (root / name).exists()]
    mapping = {str(p.relative_to(root)): file_hash(p) for p in sorted(files)}
    sha = hashlib.sha256(json.dumps(mapping, sort_keys=True).encode()).hexdigest()
    return {"identity": f"working-tree-sha256:{sha}", "files": mapping,
            "git_commit": os.environ.get("GITHUB_SHA"), "python": platform.python_version()}


def write_json(path: str | Path, value: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def backup_state(state_dir: str | Path, output: str | Path, *, quiesced: bool) -> dict:
    """Copy a stopped deployment; quiescence is an explicit operator assertion."""
    source, target = Path(state_dir).resolve(), Path(output).resolve()
    if not quiesced:
        raise ValueError("stop worker, Kafka and MLflow before backup and pass --quiesced")
    if not source.is_dir() or target.exists() or source == target or source in target.parents:
        raise ValueError("backup needs an existing source and a new destination outside that source")
    files = [p for p in source.rglob("*") if p.is_file()]
    if any(p.is_symlink() for p in source.rglob("*")):
        raise ValueError("symbolic links are not accepted in a deployment backup")
    before = {str(p.relative_to(source)): file_hash(p) for p in files}
    target.mkdir(parents=True)
    for relative in before:
        copied = target / "data" / relative
        copied.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, copied)
    after = {str(path.relative_to(source)): file_hash(path) for path in source.rglob("*")
             if path.is_file()}
    copied = {relative: file_hash(target / "data" / relative) for relative in before}
    if before != after or before != copied:
        raise RuntimeError("state changed during backup; incomplete backup must not be restored")
    manifest = {"schema_version": "1", "created_at": time.time(), "source": str(source),
                "quiescence": "operator_asserted_stopped", "files": before,
                "bytes": sum((source / p).stat().st_size for p in before)}
    write_json(target / "manifest.json", manifest)
    return manifest


def restore_state(backup: str | Path, target: str | Path) -> dict:
    """Validate every byte before copying; never overwrite an existing deployment."""
    source, destination = Path(backup).resolve(), Path(target).resolve()
    if destination.exists():
        raise ValueError("restore target already exists; select a new isolated directory")
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "1":
        raise ValueError("unknown backup manifest version")
    for relative, expected in manifest["files"].items():
        path = Path(relative)
        candidate = source / "data" / path
        resolved = candidate.resolve()
        data_root = (source / "data").resolve()
        if (path.is_absolute() or ".." in path.parts or candidate.is_symlink()
                or data_root not in resolved.parents
                or any(parent.is_symlink() for parent in candidate.parents if parent != source.parent)):
            raise ValueError("unsafe backup path")
        if file_hash(candidate) != expected:
            raise ValueError(f"backup integrity mismatch: {relative}")
    destination.mkdir(parents=True)
    for relative in manifest["files"]:
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / "data" / relative, path)
        if file_hash(path) != manifest["files"][relative]:
            raise RuntimeError(f"restored content mismatch: {relative}")
    return {"restored_files": len(manifest["files"]), "target": str(destination),
            "content_verified": True,
            "restart_status": "not_started",
            "path_binding": "Preserve original service paths or explicitly migrate checkpoint bindings."}
