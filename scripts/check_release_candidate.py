"""Validate P03 prerelease source identity and the actual generated wheel.

No network, services, account operations or package publication are involved.
Only a complete, source-matching wheel produces a verified inspection report.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from email.parser import Parser
from pathlib import Path
import re
import tomllib
from zipfile import ZipFile

PROJECT = "industrial-streaming-mlops"
DISTRIBUTION = "industrial_streaming_mlops"  # Wheel/dist-info distribution name
PACKAGE = "industrial_mlops"  # Importable package name, intentionally different
TAG = "v0.1.0-rc.1"
TAG_PATTERN = re.compile(r"^v(\d+\.\d+\.\d+)-rc\.(\d+)$")


def expected_version() -> str:
    match = TAG_PATTERN.fullmatch(TAG)
    if match is None:
        raise ValueError(f"invalid prerelease tag: {TAG}")
    return f"{match.group(1)}rc{int(match.group(2))}"


def module_version(root: Path) -> str:
    tree = ast.parse((root / "src" / PACKAGE / "__init__.py").read_text())
    values = [
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "__version__" for t in node.targets)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    ]
    if len(values) != 1:
        raise ValueError("expected exactly one literal module __version__")
    return values[0]


def inspect_source(root: Path) -> dict:
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    lock = tomllib.loads((root / "uv.lock").read_text())
    editable = [
        p for p in lock["package"]
        if p.get("name") == PROJECT and p.get("source", {}).get("editable") == "."
    ]
    version = expected_version()
    actual = {
        "declared": project.get("version"),
        "importable": module_version(root),
        "locked": editable[0].get("version") if len(editable) == 1 else None,
    }
    if any(value != version for value in actual.values()):
        raise ValueError(f"RC source/lock version mismatch: {actual}, expected {version}")
    if project.get("name") != PROJECT:
        raise ValueError("distribution name is not the expected project")
    if project.get("license") != "Apache-2.0":
        raise ValueError("project SPDX licence must be Apache-2.0")
    if set(project.get("license-files", [])) != {"LICENSE", "NOTICE"}:
        raise ValueError("project must declare both LICENSE and NOTICE")
    if project.get("scripts", {}).get("industrial-mlops") != "industrial_mlops.cli:main":
        raise ValueError("CLI entry point differs from the reviewed source")
    if TAG not in (root / "docs" / "release-notes.md").read_text():
        raise ValueError("release notes do not match planned tag")
    return {"version": version, "tag": TAG, "source_versions": actual}


def inspect_wheel(root: Path, wheel: Path) -> dict:
    source = inspect_source(root)
    version = source["version"]
    if wheel.name != f"{DISTRIBUTION}-{version}-py3-none-any.whl":
        raise ValueError(f"unexpected prerelease wheel filename: {wheel.name}")
    data = wheel.read_bytes()
    dist_info = f"{DISTRIBUTION}-{version}.dist-info"
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("duplicate wheel member paths")
        required = {
            f"{dist_info}/METADATA",
            f"{dist_info}/WHEEL",
            f"{dist_info}/RECORD",
            f"{dist_info}/entry_points.txt",
            f"{dist_info}/licenses/LICENSE",
            f"{dist_info}/licenses/NOTICE",
            f"{PACKAGE}/__init__.py",
        }
        if not required.issubset(names):
            raise ValueError(f"wheel missing required files: {sorted(required - set(names))}")
        metadata = Parser().parsestr(archive.read(f"{dist_info}/METADATA").decode("utf-8"))
        if metadata.get("Name", "").replace("_", "-").lower() != PROJECT:
            raise ValueError("wheel metadata distribution name mismatch")
        if metadata.get("Version") != version:
            raise ValueError("wheel METADATA version mismatch")
        if metadata.get("License-Expression") != "Apache-2.0":
            raise ValueError("wheel METADATA lacks Apache-2.0 License-Expression")
        license_files = {Path(p).name for p in metadata.get_all("License-File", [])}
        if not {"LICENSE", "NOTICE"}.issubset(license_files):
            raise ValueError("wheel METADATA does not declare LICENSE and NOTICE")
        for name in ("LICENSE", "NOTICE"):
            if archive.read(f"{dist_info}/licenses/{name}") != (root / name).read_bytes():
                raise ValueError(f"wheel {name} differs from the approved project source")
        if archive.read(f"{PACKAGE}/__init__.py") != (
            root / "src" / PACKAGE / "__init__.py"
        ).read_bytes():
            raise ValueError("wheel module code differs from the source revision")
        entrypoints = archive.read(f"{dist_info}/entry_points.txt").decode("utf-8")
        if "industrial-mlops = industrial_mlops.cli:main" not in entrypoints:
            raise ValueError("wheel CLI entry point differs from the project contract")
    return {
        **source, "wheel": wheel.name,
        "wheel_sha256": hashlib.sha256(data).hexdigest(),
        "wheel_bytes": len(data), "wheel_members": len(names),
        "spdx_verified": True, "license_notice_bytes_verified": True,
        "cli_entry_point_verified": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    wheels = list(args.wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        parser.error(f"expected exactly one wheel, found {len(wheels)}")
    try:
        inspection = inspect_wheel(args.root.resolve(), wheels[0])
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    inspection.update({
        "schema_version": 1, "status": "verified",
        "scope": "source/lock/PEP440 and actual wheel SPDX/NOTICE/CLI metadata",
        "limits": [
            "Not a published GitHub release, Git tag, or PyPI upload.",
            "Does not grant rights over third-party image/JVM/package components.",
            "No live Databricks, industrial field or production operations tested.",
        ],
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(inspection, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"version": inspection["version"], "tag": inspection["tag"],
                      "wheel_sha256": inspection["wheel_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
