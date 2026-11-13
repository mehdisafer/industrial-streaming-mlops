"""Bounded secret checks and installed-component inventory, not a certification.

Only repository files and reachable Git blobs are inspected. Findings contain
locations and rule names, never matched secret values. Dependency vulnerability
checking is a separate pip-audit CI step.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib


RULES = {
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"),
    "github_classic_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,255}\b"),
    "github_fine_grained_token": re.compile(r"\bgithub_pat_[A-Za-z0-9_]{60,255}\b"),
    "aws_access_key_id": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "databricks_pat": re.compile(r"\bdapi[0-9a-f]{32}\b"),
    "credential_in_url": re.compile(r"(?:postgres(?:ql)?|mysql|https?)://[^\s/:]+:[^\s/@$<>]{8,}@"),
}
MAX_FILE_BYTES = 2 * 1024 * 1024
STRONG_COPYLEFT = re.compile(r"(?<![A-Za-z])(?:AGPL|GPL)[ -](?:1|2|3)", re.I)


def git(*arguments: str) -> bytes:
    return subprocess.check_output(["git", *arguments], stderr=subprocess.PIPE)


def inspect_text(data: bytes, location: str) -> list[dict]:
    if len(data) > MAX_FILE_BYTES or b"\0" in data:
        return []
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError:
        return []
    found = []
    for number, line in enumerate(content.splitlines(), 1):
        for name, pattern in RULES.items():
            if pattern.search(line):
                fixture_path = location == "tests/test_registry.py" or location.endswith(":tests/test_registry.py")
                fixture_url = "http://" + "secret:" + "password@" + "localhost:5000"
                if name == "credential_in_url" and fixture_path and fixture_url in line:
                    continue  # Reviewed negative URL-validation fixture, never a real credential.
                found.append({"location": location, "line": number, "rule": name})
    return found


def scan_repository(history: bool) -> dict:
    findings: list[dict] = []
    scanned = 0
    skipped: list[dict] = []
    paths = git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")
    for raw_path in sorted(set(paths)):
        if not raw_path:
            continue
        path = Path(raw_path.decode())
        if not path.is_file() or path.is_symlink():
            continue
        if path.stat().st_size > MAX_FILE_BYTES:
            skipped.append({"location": path.as_posix(), "reason": "larger_than_2_MiB"})
            continue
        findings.extend(inspect_text(path.read_bytes(), path.as_posix()))
        scanned += 1
    history_blobs = 0
    if history:
        for entry in git("rev-list", "--objects", "--all").splitlines():
            parts = entry.decode().split(" ", 1)
            if len(parts) != 2:
                continue
            oid, path = parts
            if git("cat-file", "-t", oid).strip() != b"blob":
                continue
            size = int(git("cat-file", "-s", oid))
            if size > MAX_FILE_BYTES:
                skipped.append({"location": f"git:{oid}:{path}", "reason": "larger_than_2_MiB"})
                continue
            findings.extend(inspect_text(git("cat-file", "blob", oid), f"git:{oid}:{path}"))
            history_blobs += 1
    return {
        "rules": sorted(RULES),
        "worktree_files_inspected": scanned,
        "history_requested": history,
        "history_blobs_inspected": history_blobs,
        "findings": findings,
        "skipped": skipped,
        "limitations": "Pattern checks only; binary/non-UTF-8 and files over 2 MiB are not content-scanned. No entropy or hosted secret validation.",
    }


def component_inventory() -> dict:
    components = []
    lock = tomllib.loads(Path("uv.lock").read_text())
    # A tool host may expose unrelated preinstalled distributions. Inventory the
    # dependency lock, resolving the effective distribution once per name.
    for package_name in sorted({package["name"] for package in lock["package"]}):
        try:
            distribution = importlib.metadata.distribution(package_name)
        except importlib.metadata.PackageNotFoundError:
            components.append({"name": package_name, "installed": False, "version": None,
                               "review_required": True, "strong_copyleft_declaration": False})
            continue
        metadata = distribution.metadata
        name = metadata.get("Name", "unknown")
        declaration = metadata.get("License-Expression") or metadata.get("License") or ""
        classifiers = [item for item in metadata.get_all("Classifier", []) if item.startswith("License ::")]
        primary_license = metadata.get("License-Expression") or (declaration if len(declaration) < 120 else "")
        # Full wheel license notices can mention a runtime exception or another
        # bundled component. Do not misclassify a BSD project from that mention.
        strong_copyleft = bool(STRONG_COPYLEFT.search(primary_license))
        license_value = declaration if len(declaration) <= 400 else declaration[:400] + " [truncated]"
        components.append({
            "name": name,
            "installed": True,
            "version": distribution.version,
            "license_declaration": license_value or None,
            "license_classifiers": classifiers,
            "license_declaration_sha256": hashlib.sha256(declaration.encode()).hexdigest() if declaration else None,
            "review_required": (not declaration and not classifiers) or strong_copyleft,
            "strong_copyleft_declaration": strong_copyleft,
        })
    return {
        "components": sorted(components, key=lambda item: item["name"].casefold()),
        "scope": "Locked Python packages resolved in this interpreter; OS packages, JVM transitive jars and image licenses need their own inventory. Missing optional/platform packages are reported explicitly.",
        "notice": "Declared metadata is evidence for review, not a legal clearance or security certification. The repository license remains the owner's decision.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scan-history", action="store_true")
    arguments = parser.parse_args()
    report = {"schema_version": "1", "secrets": scan_repository(arguments.scan_history), "inventory": component_inventory()}
    try:
        report["git_commit"] = git("rev-parse", "HEAD").decode().strip()
    except subprocess.CalledProcessError:
        report["git_commit"] = None
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    findings = report["secrets"]["findings"]
    restricted = [item["name"] for item in report["inventory"]["components"] if item["strong_copyleft_declaration"]]
    print(json.dumps({"output": str(arguments.output), "secret_findings": len(findings), "licenses_requiring_explicit_review": restricted}))
    return 1 if findings or restricted else 0


if __name__ == "__main__":
    sys.exit(main())
