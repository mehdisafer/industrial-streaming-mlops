"""Synthetic PEP 427 wheel tests; no package publication or provider operations."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
from pathlib import Path
import sys
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_release_candidate import (  # noqa: E402
    DISTRIBUTION, PACKAGE, inspect_source, inspect_wheel,
)

VERSION = "0.1.0rc1"
TAG = "v0.1.0-rc.1"


def fixture_wheel(
    folder: Path,
    *,
    metadata_version: str = VERSION,
    include_notice: bool = True,
    notice_override: bytes | None = None,
    tamper_module: bool = False,
    missing_module: bool = False,
    extra_module: bool = False,
    corrupt_record: bool = False,
    entry_points_override: str | None = None,
    extra_member: str | None = None,
) -> Path:
    """Build a complete synthetic wheel with a valid hashed RECORD by default."""
    folder.mkdir(parents=True, exist_ok=True)
    wheel = folder / f"{DISTRIBUTION}-{VERSION}-py3-none-any.whl"
    prefix = f"{DISTRIBUTION}-{VERSION}.dist-info"
    entries: dict[str, bytes] = {
        f"{prefix}/METADATA": (
            "Metadata-Version: 2.4\nName: industrial-streaming-mlops\n"
            f"Version: {metadata_version}\nLicense-Expression: Apache-2.0\n"
            "License-File: LICENSE\nLicense-File: NOTICE\n"
        ).encode(),
        f"{prefix}/WHEEL": (
            b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        f"{prefix}/entry_points.txt": (
            entry_points_override or
            "[console_scripts]\nindustrial-mlops = industrial_mlops.cli:main\n"
        ).encode(),
        f"{prefix}/licenses/LICENSE": (ROOT / "LICENSE").read_bytes(),
    }
    if include_notice:
        entries[f"{prefix}/licenses/NOTICE"] = (
            notice_override if notice_override is not None else (ROOT / "NOTICE").read_bytes()
        )

    package_dir = ROOT / "src" / PACKAGE
    for file in sorted(package_dir.rglob("*.py")):
        if "__pycache__" not in file.parts:
            entries[file.relative_to(ROOT / "src").as_posix()] = file.read_bytes()
    if tamper_module:
        entries[f"{PACKAGE}/features.py"] += b"\n# unreviewed tampered module\n"
    if missing_module:
        entries.pop(f"{PACKAGE}/features.py")
    if extra_module:
        entries[f"{PACKAGE}/unreviewed.py"] = b"print('unexpected')\n"
    if extra_member:
        entries[extra_member] = b"out-of-scope"

    record = io.StringIO()
    writer = csv.writer(record, lineterminator="\n")
    for name, payload in entries.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode().rstrip("=")
        if corrupt_record and name.endswith("/WHEEL"):
            digest = "A" * 43
        writer.writerow([name, f"sha256={digest}", str(len(payload))])
    writer.writerow([f"{prefix}/RECORD", "", ""])
    entries[f"{prefix}/RECORD"] = record.getvalue().encode()

    with ZipFile(wheel, "w") as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return wheel


def test_distribution_and_importable_package_are_distinct() -> None:
    assert DISTRIBUTION == "industrial_streaming_mlops"
    assert PACKAGE == "industrial_mlops"


def test_source_lock_module_and_prepared_tag_agree() -> None:
    result = inspect_source(ROOT)
    assert result["version"] == VERSION
    assert result["tag"] == TAG
    assert set(result["source_versions"].values()) == {VERSION}


def test_synthetic_wheel_matches_complete_package_and_hashed_record(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path)
    result = inspect_wheel(ROOT, wheel)
    assert result["spdx_verified"] is True
    assert result["license_notice_bytes_verified"] is True
    assert result["cli_entry_point_verified"] is True
    assert result["source_package_bytes_verified"] is True
    assert result["wheel_record_sha256_verified"] is True
    assert result["package_modules_verified"] >= 16
    assert result["record_entries_verified"] >= 20
    assert len(result["wheel_sha256"]) == 64


def test_wheel_with_wrong_metadata_version_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, metadata_version="0.1.0")
    with pytest.raises(ValueError, match="METADATA version mismatch"):
        inspect_wheel(ROOT, wheel)


def test_wheel_without_notice_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, include_notice=False)
    with pytest.raises(ValueError, match="missing required files"):
        inspect_wheel(ROOT, wheel)


def test_wheel_with_tampered_notice_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, notice_override=b"invalid notice\n")
    with pytest.raises(ValueError, match="wheel NOTICE differs"):
        inspect_wheel(ROOT, wheel)


def test_unreviewed_module_change_with_valid_record_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, tamper_module=True)
    with pytest.raises(ValueError, match="wheel package module differs"):
        inspect_wheel(ROOT, wheel)


def test_missing_source_module_with_valid_record_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, missing_module=True)
    with pytest.raises(ValueError, match="module set differs"):
        inspect_wheel(ROOT, wheel)


def test_extra_package_module_with_valid_record_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, extra_module=True)
    with pytest.raises(ValueError, match="module set differs"):
        inspect_wheel(ROOT, wheel)


def test_record_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, corrupt_record=True)
    with pytest.raises(ValueError, match="RECORD digest/size mismatch"):
        inspect_wheel(ROOT, wheel)


def test_cli_substring_spoof_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(
        tmp_path,
        entry_points_override="[console_scripts]\n"
                              "industrial-mlops = industrial_mlops.cli:main_malicious\n",
    )
    with pytest.raises(ValueError, match="CLI entry point differs"):
        inspect_wheel(ROOT, wheel)


def test_wheel_traversal_member_is_rejected(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path, extra_member="../outside.py")
    with pytest.raises(ValueError, match="unsafe or non-file wheel member"):
        inspect_wheel(ROOT, wheel)
