"""Exercise production scope selection against real temporary Git histories."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TestCIScope(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="industrial-ci-scope-")
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.commit("src/runtime.py", "initial = True\n")
        self.base = self.commit("README.md", "Initial reference\n")

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=CI Scope Test", "-c", "user.email=ci@example.invalid", *args],
            cwd=self.repo, text=True,
        ).strip()

    def commit(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        self.git("add", name)
        self.git("commit", "--quiet", "-m", "scope fixture")
        return self.git("rev-parse", "HEAD")

    def run_scope(self, event_name, event, *, with_event=True):
        event_path = self.repo / "event.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")
        output = self.repo / "output.txt"
        output.write_text("", encoding="utf-8")
        env = {**os.environ, "GITHUB_EVENT_NAME": event_name,
               "GITHUB_SHA": self.git("rev-parse", "HEAD"), "GITHUB_OUTPUT": str(output)}
        env.pop("GITHUB_EVENT_PATH", None)
        if with_event:
            env["GITHUB_EVENT_PATH"] = str(event_path)
        completed = subprocess.run(
            [sys.executable, str(ROOT / "scripts/ci_scope.py")], cwd=self.repo,
            env=env, capture_output=True, text=True, timeout=30, check=True,
        )
        report = json.loads(completed.stdout)
        self.assertEqual(report, json.loads((self.repo / "evidence/ci/scope.json").read_text()))
        self.assertEqual(output.read_text(), f"integration={str(report['integration']).lower()}\n")
        self.assertEqual(report["checkout_sha"], self.git("rev-parse", "HEAD"))
        return report

    def pr_event(self, head, **overrides):
        return {"action": "synchronize", "before": self.base, "after": head,
                "pull_request": {"base": {"sha": self.base}, "head": {"sha": head}}, **overrides}

    def test_docs_synchronize_keeps_earlier_runtime_change(self):
        before = self.commit("src/runtime.py", "changed = True\n")
        head = self.commit("README.md", "Follow-up documentation\n")
        report = self.run_scope("pull_request", self.pr_event(head, before=before))
        self.assertTrue(report["integration"])
        self.assertEqual(report["selection"], "full-pr-diff")
        self.assertEqual(report["base_sha"], self.base)
        self.assertEqual(report["changed_paths"], ["README.md", "src/runtime.py"])

    def test_docs_only_pr_stays_lightweight(self):
        head = self.commit("README.md", "Documentation only\n")
        report = self.run_scope("pull_request", self.pr_event(head))
        self.assertFalse(report["integration"])
        self.assertEqual(report["selection"], "full-pr-diff")

    def test_multi_commit_push_includes_runtime_before_final_docs_commit(self):
        self.commit("src/runtime.py", "changed = True\n")
        head = self.commit("README.md", "Follow-up documentation\n")
        report = self.run_scope("push", {"before": self.base, "after": head})
        self.assertTrue(report["integration"])
        self.assertEqual(report["selection"], "branch-push-diff")

    def test_inconsistent_pr_head_never_skips(self):
        head = self.commit("README.md", "Documentation only\n")
        report = self.run_scope("pull_request", self.pr_event(head, after=self.base))
        self.assertTrue(report["integration"])
        self.assertEqual(report["selection"], "inconsistent-pr-head-full-qualification")

    def test_unknown_or_zero_before_never_skips(self):
        for before in ("0" * 40, "f" * 40, "--invalid-ref"):
            with self.subTest(before=before):
                report = self.run_scope("push", {"before": before, "after": self.base})
                self.assertTrue(report["integration"])
                self.assertEqual(report["selection"], "unavailable-reference-full-qualification")

    def test_manual_and_missing_metadata_never_skip(self):
        for name, present in (("workflow_dispatch", True), ("pull_request", False)):
            with self.subTest(event=name):
                report = self.run_scope(name, {}, with_event=present)
                self.assertTrue(report["integration"])

    def test_renaming_runtime_to_docs_keeps_removed_input_in_scope(self):
        self.git("mv", "src/runtime.py", "runtime-reference.md")
        self.git("commit", "--quiet", "-m", "move runtime input")
        report = self.run_scope("pull_request", self.pr_event(self.git("rev-parse", "HEAD")))
        self.assertTrue(report["integration"])
        self.assertIn("src/runtime.py", report["changed_paths"])

    def test_workflow_change_requires_integration(self):
        head = self.commit(".github/workflows/ci.yml", "name: changed workflow\n")
        report = self.run_scope("pull_request", self.pr_event(head))
        self.assertTrue(report["integration"])


if __name__ == "__main__":
    unittest.main()
