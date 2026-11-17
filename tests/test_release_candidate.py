"""Synthetic release wheel fixtures; never publish a distribution."""
from __future__ import annotations

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


def fixture_wheel(folder: Path, *, metadata_version: str = VERSION,
                  include_notice: bool = True, notice_override: bytes | None = None) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    wheel = folder / f"industrial_streaming_mlops-{VERSION}-py3-none-any.whl"
    prefix = f"industrial_streaming_mlops-{VERSION}.dist-info"
    with ZipFile(wheel, "w") as archive:
        archive.writestr(
            f"{prefix}/METADATA",
            "Metadata-Version: 2.4\nName: industrial-streaming-mlops\n"
            f"Version: {metadata_version}\nLicense-Expression: Apache-2.0\n"
            "License-File: LICENSE\nLicense-File: NOTICE\n",
        )
        archive.writestr(f"{prefix}/WHEEL", "Wheel-Version: 1.0\n")
        archive.writestr(f"{prefix}/RECORD", "")
        archive.writestr(
            f"{prefix}/entry_points.txt",
            "[console_scripts]\nindustrial-mlops = industrial_mlops.cli:main\n",
        )
        archive.writestr(
            "industrial_mlops/__init__.py",
            (ROOT / "src/industrial_mlops/__init__.py").read_bytes(),
        )
        archive.writestr(f"{prefix}/licenses/LICENSE", (ROOT / "LICENSE").read_bytes())
        if include_notice:
            archive.writestr(
                f"{prefix}/licenses/NOTICE",
                notice_override if notice_override is not None else (ROOT / "NOTICE").read_bytes(),
            )
    return wheel


def test_distribution_and_importable_package_are_distinct() -> None:
    assert DISTRIBUTION == "industrial_streaming_mlops"
    assert PACKAGE == "industrial_mlops"


def test_source_lock_module_and_prepared_tag_agree() -> None:
    result = inspect_source(ROOT)
    assert result["version"] == VERSION
    assert result["tag"] == TAG
    assert set(result["source_versions"].values()) == {VERSION}


def test_synthetic_wheel_fixture_passes_exact_source_and_notice_checks(tmp_path: Path) -> None:
    wheel = fixture_wheel(tmp_path)
    result = inspect_wheel(ROOT, wheel)
    assert result["spdx_verified"] is True
    assert result["license_notice_bytes_verified"] is True
    assert result["cli_entry_point_verified"] is True
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
