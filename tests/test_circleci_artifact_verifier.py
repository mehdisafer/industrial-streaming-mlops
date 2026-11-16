"""Offline fixture tests; these do not assert access to original CircleCI outputs."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.verify_circleci_artifacts import CHECKS, verify  # noqa: E402

SOURCE = "4bcc55ec470a752bac59dfa45f561dad0261d95e"


class CircleCIArtifactVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.artifacts = root / "integration"
        self.artifacts.mkdir()
        self.host = root / "ci-host"
        self.host.mkdir()
        self.manifest = root / "manifest.json"
        self.receipts = {
            "run-receipt.json": {
                "git_commit": SOURCE, "exit_code": 0, "stage": "complete",
                "container_smoke_completed": True, "namespace_ownership_checked": True,
                "stack_start_attempted": True, "cleanup_attempted": True,
                "cleanup_exit_code": 0, "runtime_inventory_capture": "complete",
                "preflight_only": False,
            },
            "final-verification.json": {
                "valid": True, "checks": dict.fromkeys(CHECKS, True),
                "counts": {
                    "raw": 100, "accepted": 45, "label": 45, "rejected": 5,
                    "duplicate": 3, "control": 2, "events": 45, "labels": 45,
                    "features": 40, "predictions": 40, "alerts": 2,
                    "pending_events": 0, "pending_predictions": 0,
                },
            },
            "lifecycle-assertions.json": {
                "rollback_observed": True, "real_prediction_versions": {"1": 10, "2": 30},
            },
            "recovery-assertions.json": {
                "source_reconciliation": True, "after_commit_recovery": True,
                "acknowledged": 80,
            },
        }
        self.save()

    def save(self, *, pin=True) -> None:
        for name, value in self.receipts.items():
            (self.artifacts / name).write_text(json.dumps(value, indent=2) + "\n")
        digests = {name: hashlib.sha256((self.artifacts / name).read_bytes()).hexdigest()
                   for name in self.receipts}
        self.manifest.write_text(json.dumps({
            "schema_version": 1, "source_commit": SOURCE,
            "expected_sha256": digests if pin else {},
        }))

    def test_verified_four_receipts_are_source_bound_and_reconciled(self):
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "verified", result["errors"])
        self.assertTrue(result["pinned_digest_baseline"])
        self.assertEqual(len(result["sha256_observed"]), 4)

    def test_absent_independent_hash_baseline_cannot_pass_integrity(self):
        self.save(pin=False)
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "observed_unpinned")
        self.assertFalse(result["pinned_digest_baseline"])

    def test_tampered_artifact_fails_even_if_the_json_is_valid(self):
        (self.artifacts / "lifecycle-assertions.json").write_text(json.dumps(
            self.receipts["lifecycle-assertions.json"]))
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("hash differs" in e for e in result["errors"]))

    def test_invariant_failure_fails_with_fresh_matching_hash(self):
        self.receipts["final-verification.json"]["counts"]["raw"] = 99
        self.save()
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("reconciliation failed" in e for e in result["errors"]))

    def test_empty_json_receipt_is_not_a_pass_even_if_pinned(self):
        self.receipts["run-receipt.json"] = {}
        self.save()
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("empty JSON object" in e for e in result["errors"]))

    def test_added_failed_check_is_not_ignored(self):
        self.receipts["final-verification.json"]["checks"]["new_check"] = False
        self.save()
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("one or more invariants" in e for e in result["errors"]))

    def test_missing_named_check_is_not_a_pass(self):
        self.receipts["final-verification.json"]["checks"].pop("sqlite_integrity")
        self.save()
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("named verification" in e for e in result["errors"]))

    def test_wrong_source_with_matching_hash_is_not_a_pass(self):
        self.receipts["run-receipt.json"]["git_commit"] = "a" * 40
        self.save()
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("git_commit" in e for e in result["errors"]))

    def test_invalid_historical_sha256_length_rejected_as_baseline(self):
        spec = json.loads(self.manifest.read_text())
        spec["expected_sha256"]["run-receipt.json"] += "a"
        self.manifest.write_text(json.dumps(spec))
        with self.assertRaisesRegex(ValueError, "invalid pinned SHA-256"):
            verify(self.artifacts, self.manifest)

    def test_missing_receipt_and_symlink_never_pass(self):
        (self.artifacts / "run-receipt.json").unlink()
        outside = self.host / "run-receipt.json"
        outside.write_text("{}")
        (self.artifacts / "run-receipt.json").symlink_to(outside)
        result = verify(self.artifacts, self.manifest)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("unsafe file" in e for e in result["errors"]))

    def test_full_evidence_requires_host_and_runtime_inventory(self):
        result = verify(self.artifacts, self.manifest, full=True)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any("--host" in e for e in result["errors"]))

    def test_bad_dashboard_json_is_not_full_evidence(self):
        self.test_full_evidence_verifies_inventory_and_host_identity()
        (self.artifacts / "dashboard-snapshot.json").write_text("not json")
        result = verify(self.artifacts, self.manifest, full=True, host=self.host)
        self.assertEqual(result["status"], "failed")

    def test_full_evidence_verifies_inventory_and_host_identity(self):
        for name in ("pre-cleanup-images.json", "pre-cleanup-container.log",
                     "dashboard-snapshot.json", "cleanup.log"):
            (self.artifacts / name).write_text(
                "[]\n" if name == "pre-cleanup-images.json" else
                "{}\n" if name == "dashboard-snapshot.json" else "fixture\n")
        inventory = self.artifacts / "runtime-inventory"
        inventory.mkdir()
        entries = []
        for name in ("runtime-jars.json", "os-packages.tsv", "java-version.txt"):
            data = b"fixture\n"
            (inventory / name).write_bytes(data)
            entries.append({"path": f"runtime-inventory/{name}", "captured": True,
                            "copy_exit_code": 0, "bytes": len(data),
                            "sha256": hashlib.sha256(data).hexdigest()})
        (self.artifacts / "runtime-inventory-capture.json").write_text(
            json.dumps({"complete": True, "files": entries}))
        for name in ("docker-version.txt", "compose-version.txt", "kernel.txt"):
            (self.host / name).write_text("fixture\n")
        (self.host / "commit.txt").write_text(SOURCE + "\n")
        result = verify(self.artifacts, self.manifest, full=True, host=self.host)
        self.assertEqual(result["status"], "verified", result["errors"])
        self.assertTrue(result["inventory_verified"])


if __name__ == "__main__":
    unittest.main()
