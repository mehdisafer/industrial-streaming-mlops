"""Check the approved project licence without interpreting third-party rights."""
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def test_project_metadata_declares_apache_2_with_license_files() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["license"] == "Apache-2.0"
    assert project["license-files"] == ["LICENSE", "NOTICE"]
    assert project["authors"] == [{"name": "Mehdi Safer"}]


def test_full_apache_license_is_present() -> None:
    license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert "Apache License" in license_text
    assert "Version 2.0, January 2004" in license_text
    assert "TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION" in license_text
    assert "END OF TERMS AND CONDITIONS" in license_text
    assert "APPENDIX: How to apply the Apache License to your work." in license_text
    for section in range(1, 10):
        assert f"   {section}. " in license_text


def test_notice_attributes_project_without_relicensing_dependencies() -> None:
    notice = (ROOT / "NOTICE").read_text(encoding="utf-8")
    assert "Copyright 2026 Mehdi Safer" in notice
    assert "Apache License, Version 2.0" in notice
    assert "third-party components remain subject to their respective licenses" in notice
