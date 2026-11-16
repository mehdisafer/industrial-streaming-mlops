"""Verify downloaded CircleCI Docker receipts offline; never fetch or mutate services.

This verifies original bytes and selected invariants, not source authorship,
provider execution, model fitness, or a release acceptance decision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re

REQUIRED = frozenset({
    "run-receipt.json", "final-verification.json",
    "lifecycle-assertions.json", "recovery-assertions.json",
})
CHECKS = frozenset({
    "source_dispositions_balance", "accepted_events_balance", "labels_balance",
    "predictions_have_features", "alerts_have_predictions", "sqlite_integrity",
})
SHA = re.compile(r"[0-9a-f]{64}\Z")


def _read(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"missing or unsafe file: {path.name}")
    data = path.read_bytes()
    if not data:
        raise ValueError(f"empty file: {path.name}")
    return data


def verify(artifacts: Path, manifest: Path, *, full: bool = False,
           host: Path | None = None) -> dict:
    """Return structured findings; only status=verified indicates this scope passed."""
    errors: list[str] = []
    hashes: dict[str, str] = {}
    records: dict[str, dict] = {}
    specification = json.loads(_read(manifest))
    expected = specification.get("expected_sha256", {})
    commit = specification.get("source_commit")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("manifest requires a 40-character source commit")
    if not isinstance(expected, dict) or (expected and set(expected) != REQUIRED):
        raise ValueError("manifest must have either no pinned digests or all four")
    if any(not isinstance(value, str) or not SHA.fullmatch(value)
           for value in expected.values()):
        raise ValueError("manifest includes an invalid pinned SHA-256")
    for name in sorted(REQUIRED):
        try:
            data = _read(artifacts / name)
            digest = hashlib.sha256(data).hexdigest()
            hashes[name] = digest
            if expected and digest != expected[name]:
                errors.append(f"{name}: original byte hash differs from pinned SHA-256")
            obj = json.loads(data)
            if not isinstance(obj, dict):
                raise ValueError("expected a JSON object")
            records[name] = obj
        except (ValueError, OSError, UnicodeDecodeError) as exc:
            errors.append(f"{name}: {exc}")
    run = records.get("run-receipt.json", {})
    required_run = {
        "git_commit": commit, "exit_code": 0, "stage": "complete",
        "container_smoke_completed": True, "namespace_ownership_checked": True,
        "stack_start_attempted": True, "cleanup_attempted": True,
        "cleanup_exit_code": 0, "runtime_inventory_capture": "complete",
        "preflight_only": False,
    }
    for key, value in required_run.items():
        if key in run and (type(run[key]) is not type(value) or run[key] != value):
            errors.append(f"run-receipt.json: invalid {key}")
        elif key not in run and run:
            errors.append(f"run-receipt.json: missing {key}")
    final = records.get("final-verification.json", {})
    if final:
        checks, counts = final.get("checks"), final.get("counts")
        if final.get("valid") is not True or not isinstance(checks, dict) or not CHECKS <= checks.keys():
            errors.append("final-verification.json: incomplete or invalid named verification")
        elif any(checks[name] is not True for name in CHECKS):
            errors.append("final-verification.json: one or more invariants are not true")
        if not isinstance(counts, dict):
            errors.append("final-verification.json: missing counts")
        else:
            needed = {"raw", "accepted", "rejected", "duplicate", "label", "control",
                      "events", "labels", "features", "predictions", "alerts",
                      "pending_predictions", "pending_events"}
            if not needed <= counts.keys() or any(type(counts[k]) is not int or counts[k] < 0 for k in needed if k in counts):
                errors.append("final-verification.json: missing or invalid counters")
            elif not (counts["raw"] > 0 and counts["raw"] == sum(counts[k] for k in
                      ("accepted", "rejected", "duplicate", "label", "control"))
                      and counts["accepted"] == counts["events"]
                      and counts["label"] == counts["labels"]
                      and counts["features"] == counts["predictions"] > 0
                      and 0 < counts["alerts"] <= counts["predictions"]
                      and counts["pending_predictions"] == counts["pending_events"] == 0):
                errors.append("final-verification.json: source/output reconciliation failed")
    lifecycle = records.get("lifecycle-assertions.json", {})
    if lifecycle:
        versions = lifecycle.get("real_prediction_versions")
        if lifecycle.get("rollback_observed") is not True or not isinstance(versions, dict) or len(versions) < 2 or any(type(n) is not int or n <= 0 for n in versions.values()):
            errors.append("lifecycle-assertions.json: rollback and version evidence incomplete")
    recovery = records.get("recovery-assertions.json", {})
    if recovery:
        if recovery.get("source_reconciliation") is not True or recovery.get("after_commit_recovery") is not True or type(recovery.get("acknowledged")) is not int or recovery["acknowledged"] <= 0:
            errors.append("recovery-assertions.json: incomplete recovery evidence")
        elif isinstance(final.get("counts"), dict) and type(final["counts"].get("raw")) is int and recovery["acknowledged"] > final["counts"]["raw"]:
            errors.append("recovery-assertions.json: acknowledged exceeds final raw count")
    inventory_ok = False
    if full:
        required_extra = ("pre-cleanup-images.json", "pre-cleanup-container.log",
                          "dashboard-snapshot.json", "cleanup.log", "runtime-inventory-capture.json")
        for name in required_extra:
            try:
                _read(artifacts / name)
            except (OSError, ValueError) as exc:
                errors.append(f"full evidence: {exc}")
        try:
            inventory = json.loads(_read(artifacts / "runtime-inventory-capture.json"))
            entries = inventory["files"]
            if inventory.get("complete") is not True or not isinstance(entries, list) or len(entries) != 3:
                raise ValueError("runtime inventory is incomplete")
            names = {"runtime-jars.json", "os-packages.tsv", "java-version.txt"}
            if {Path(item["path"]).name for item in entries} != names:
                raise ValueError("runtime inventory has unexpected files")
            for item in entries:
                name = Path(item["path"]).name
                data = _read(artifacts / "runtime-inventory" / name)
                if item.get("captured") is not True or item.get("copy_exit_code") != 0 or item.get("bytes") != len(data) or item.get("sha256") != hashlib.sha256(data).hexdigest():
                    raise ValueError(f"runtime inventory mismatch: {name}")
            inventory_ok = True
        except (KeyError, TypeError, ValueError, OSError) as exc:
            errors.append(f"full evidence: {exc}")
        if host is None:
            errors.append("full evidence: --host directory is required")
        else:
            for name in ("commit.txt", "docker-version.txt", "compose-version.txt", "kernel.txt"):
                try:
                    data = _read(host / name)
                    if name == "commit.txt" and data.decode().strip() != commit:
                        errors.append("full evidence: host commit differs from source")
                except (ValueError, OSError, UnicodeDecodeError) as exc:
                    errors.append(f"full evidence host: {exc}")
    status = "failed" if errors else ("verified" if expected else "observed_unpinned")
    return {
        "schema_version": 1, "scope": "offline CircleCI original bytes and selected invariants",
        "source_commit": commit, "level": "full" if full else "four_receipts",
        "status": status, "sha256_observed": hashes,
        "pinned_digest_baseline": bool(expected),
        "inventory_verified": inventory_ok if full else None, "errors": errors,
        "limitations": ["No live Kafka, Docker, Databricks or browser execution is performed.",
                        "Observed digests without a separately reviewed pinned baseline do not pass integrity acceptance.",
                        "A verified receipt is not proof of authorship, security or provider acceptance."],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--manifest", type=Path, default=Path("docs/evidence/circleci/manifest.json"))
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--host", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.artifacts, args.manifest, full=args.full, host=args.host)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"status": result["status"], "level": result["level"],
                      "errors": result["errors"]}, sort_keys=True))
    return 0 if result["status"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
