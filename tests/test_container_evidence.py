"""Shell lifecycle checks with Docker/curl/port-preflight doubles; no containers run."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
INVENTORIES = {
    "runtime-jars.json": json.dumps([{"file": "fixture.jar", "sha256": "a" * 64}]) + "\n",
    "os-packages.tsv": "fixture-package\t1.0\n",
    "java-version.txt": 'openjdk version "17.test-fixture"\n',
}
DOCKER_DOUBLE = r'''
import json, os, pathlib, sys
arguments = sys.argv[1:]
with open(os.environ['MOCK_COMMAND_LOG'], 'a') as stream:
    stream.write(json.dumps(arguments) + '\n')
if arguments[:2] in [['container', 'ls'], ['volume', 'ls'], ['network', 'ls']]:
    raise SystemExit(0)
if arguments[:2] == ['image', 'inspect']:
    print('[]')
    raise SystemExit(0)
assert arguments[0] == 'compose', arguments
arguments = arguments[1:]
if arguments[:1] == ['--project-name']:
    arguments = arguments[2:]
while arguments[:1] == ['--profile']:
    arguments = arguments[2:]
command = arguments[0]
if command == 'version':
    print('mock-compose-version')
elif command == 'build':
    raise SystemExit(31 if os.environ.get('MOCK_FAIL_STAGE') == 'build' else 0)
elif command == 'up':
    raise SystemExit(32 if os.environ.get('MOCK_FAIL_STAGE') == 'startup' else 0)
elif command == 'run':
    if '--entrypoint' in arguments:
        code = arguments[-1]
        if "['qualification']['eligible']" in code:
            print('true')
        elif "['baseline']['model_version']" in code:
            print('1')
        elif "['candidate']['model_version']" in code:
            print('2')
        else:
            print('{"scope":"mocked CLI response; assertions not executed"}')
    elif '--failpoint' in arguments:
        raise SystemExit(19)
    elif 'generate' in arguments and os.environ.get('MOCK_FAIL_STAGE') == 'generate':
        raise SystemExit(42)
    else:
        print('{}')
elif command == 'cp':
    source, destination = arguments[-2:]
    path = pathlib.Path(destination)
    if source.startswith('dashboard:/app/'):
        name = source.rsplit('/', 1)[-1]
        payloads = json.loads(os.environ['MOCK_INVENTORIES'])
        assert name in payloads, name
        if os.environ.get('MOCK_FAIL_STAGE') == 'startup':
            raise SystemExit(74)
        if os.environ.get('MOCK_COPY_FAILURE') == name:
            path.write_bytes(b'partial-copy')
            raise SystemExit(73)
        if os.environ.get('MOCK_EMPTY_COPY') == name:
            path.write_bytes(b'')
        elif os.environ.get('MOCK_INVALID_JSON') == name:
            path.write_text('not-json')
        else:
            path.write_text(payloads[name])
    elif source.endswith('/.'):
        path.mkdir(parents=True, exist_ok=True)
        (path / 'mock-service-receipt.json').write_text('{"mock":true}\n')
    else:
        path.write_text('mock holdout; no real model execution\n')
elif command == 'ps':
    print('[]')
elif command == 'logs':
    print('mock service log; no containers executed')
elif command in ('kill', 'down'):
    pass
else:
    raise AssertionError(arguments)
'''
PYTHON_DOUBLE = r'''
import json, os, pathlib, sys
if len(sys.argv) == 3 and sys.argv[1] == '-' and sys.argv[2].endswith('ports-preflight.json'):
    code = sys.stdin.read()
    assert 'probe.bind' in code and '(5000, 8080, 19092)' in code
    pathlib.Path(sys.argv[2]).write_text(json.dumps({
        'ports': [5000, 8080, 19092], 'failures': [],
        'scope': 'Explicit mocked port preflight; no socket bind was executed.',
    }) + '\n')
else:
    # All other script Python, including inventory hashing and receipts, is real.
    os.execv(os.environ['MOCK_REAL_PYTHON'], [os.environ['MOCK_REAL_PYTHON'], *sys.argv[1:]])
'''


@unittest.skipUnless(shutil.which("bash"), "container shell checks require Bash")
class ContainerInventoryEvidenceTests(unittest.TestCase):
    def run_mock_smoke(self, *, preflight=False, **settings):
        with tempfile.TemporaryDirectory(prefix="industrial-container-evidence-") as temporary:
            directory = Path(temporary)
            (directory / "scripts").mkdir()
            script = directory / "scripts" / "integration.sh"
            shutil.copyfile(ROOT / "scripts" / "integration.sh", script)
            executable_dir = directory / "bin"
            executable_dir.mkdir()
            for name, body in {
                "docker": DOCKER_DOUBLE,
                "curl": "print('{\"mock_dashboard\":true}')\n",
                "python3": PYTHON_DOUBLE,
            }.items():
                executable = executable_dir / name
                executable.write_text(f"#!{sys.executable}\n" + body)
                executable.chmod(0o700)
            evidence = directory / "evidence"
            commands = directory / "commands.jsonl"
            environment = dict(os.environ)
            for key in list(environment):
                if key.startswith("MOCK_"):
                    del environment[key]
            environment.update({
                "PATH": str(executable_dir) + os.pathsep + os.environ.get("PATH", ""),
                "COMPOSE_PROJECT_NAME": "industrial-mocked-inventory",
                "EVIDENCE_DIR": str(evidence),
                "MOCK_COMMAND_LOG": str(commands),
                "MOCK_INVENTORIES": json.dumps(INVENTORIES),
                "MOCK_REAL_PYTHON": sys.executable,
                **settings,
            })
            result = subprocess.run(
                ["bash", str(script), *(["--preflight-only"] if preflight else [])],
                cwd=directory, env=environment, text=True, capture_output=True, timeout=20,
            )
            receipt = json.loads((evidence / "run-receipt.json").read_text())
            inventory_path = evidence / "runtime-inventory-capture.json"
            inventory = json.loads(inventory_path.read_text()) if inventory_path.exists() else None
            calls = [json.loads(line) for line in commands.read_text().splitlines()]
            captured = {
                path.name: path.read_bytes()
                for path in (evidence / "runtime-inventory").glob("*") if path.is_file()
            }
            return result, receipt, inventory, calls, captured

    def assert_capture_precedes_cleanup(self, calls):
        copies = [index for index, call in enumerate(calls)
                  if any(argument.startswith("dashboard:/app/") for argument in call)]
        cleanup = [index for index, call in enumerate(calls) if "down" in call]
        self.assertEqual(len(copies), 3)
        self.assertEqual(len(cleanup), 1)
        self.assertLess(max(copies), cleanup[0])

    def test_complete_inventory_copies_preserve_original_bytes_and_hashes(self):
        result, receipt, inventory, calls, captured = self.run_mock_smoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(receipt["container_smoke_completed"])
        self.assertEqual(receipt["runtime_inventory_capture"], "complete")
        self.assertEqual(receipt["runtime_inventory_receipt"], "runtime-inventory-capture.json")
        self.assertTrue(inventory["complete"])
        self.assertEqual(inventory["status"], "complete")
        for item in inventory["files"]:
            name = item["path"].rsplit("/", 1)[-1]
            expected = INVENTORIES[name].encode()
            self.assertEqual(captured[name], expected)
            self.assertEqual(item["sha256"], hashlib.sha256(expected).hexdigest())
            self.assertEqual(item["bytes"], len(expected))
            self.assertEqual(item["copy_exit_code"], 0)
            self.assertIsNone(item["failure_reason"])
        self.assert_capture_precedes_cleanup(calls)

    def test_copy_failure_fails_the_run_without_losing_partial_evidence_or_cleanup(self):
        result, receipt, inventory, calls, captured = self.run_mock_smoke(
            MOCK_COPY_FAILURE="os-packages.tsv")
        self.assertEqual(result.returncode, 1)
        self.assertFalse(receipt["container_smoke_completed"])
        self.assertEqual(receipt["runtime_inventory_capture"], "failed")
        self.assertEqual(receipt["cleanup_exit_code"], 0)
        self.assertFalse(inventory["complete"])
        failed = next(item for item in inventory["files"] if not item["captured"])
        self.assertEqual(failed["copy_exit_code"], 73)
        self.assertEqual(failed["failure_reason"], "copy_failed")
        self.assertEqual(captured["os-packages.tsv"], b"partial-copy")
        self.assert_capture_precedes_cleanup(calls)

    def test_empty_successful_copy_is_not_complete_evidence(self):
        result, receipt, inventory, calls, _ = self.run_mock_smoke(
            MOCK_EMPTY_COPY="java-version.txt")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(receipt["runtime_inventory_capture"], "failed")
        failed = next(item for item in inventory["files"] if not item["captured"])
        self.assertEqual(failed["copy_exit_code"], 0)
        self.assertEqual(failed["failure_reason"], "empty_or_missing_file")
        self.assert_capture_precedes_cleanup(calls)

    def test_invalid_jar_json_is_not_complete_evidence(self):
        result, _, inventory, calls, _ = self.run_mock_smoke(
            MOCK_INVALID_JSON="runtime-jars.json")
        self.assertEqual(result.returncode, 1)
        failed = next(item for item in inventory["files"] if not item["captured"])
        self.assertEqual(failed["failure_reason"], "invalid_jar_json")
        self.assert_capture_precedes_cleanup(calls)

    def test_inventory_failure_preserves_the_earlier_operational_exit_code(self):
        result, receipt, inventory, calls, _ = self.run_mock_smoke(
            MOCK_FAIL_STAGE="generate", MOCK_COPY_FAILURE="os-packages.tsv")
        self.assertEqual(result.returncode, 42)
        self.assertEqual(receipt["exit_code"], 42)
        self.assertEqual(receipt["stage"], "generate-and-train")
        self.assertFalse(receipt["container_smoke_completed"])
        self.assertFalse(inventory["complete"])
        self.assertEqual(receipt["cleanup_exit_code"], 0)
        self.assert_capture_precedes_cleanup(calls)

    def test_startup_failure_retains_failed_captures_and_still_cleans_up(self):
        result, receipt, inventory, calls, _ = self.run_mock_smoke(MOCK_FAIL_STAGE="startup")
        self.assertEqual(result.returncode, 32)
        self.assertEqual(receipt["stage"], "startup")
        self.assertTrue(receipt["cleanup_attempted"])
        self.assertFalse(inventory["complete"])
        self.assertTrue(all(item["copy_exit_code"] == 74 for item in inventory["files"]))
        self.assert_capture_precedes_cleanup(calls)

    def test_build_failure_does_not_claim_or_attempt_container_inventory(self):
        result, receipt, inventory, calls, _ = self.run_mock_smoke(MOCK_FAIL_STAGE="build")
        self.assertEqual(result.returncode, 31)
        self.assertEqual(receipt["runtime_inventory_capture"], "not_attempted")
        self.assertFalse(receipt["cleanup_attempted"])
        self.assertIsNone(inventory)
        self.assertFalse(any("down" in call or "cp" in call for call in calls))

    def test_preflight_does_not_claim_or_attempt_container_inventory(self):
        result, receipt, inventory, calls, _ = self.run_mock_smoke(preflight=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(receipt["runtime_inventory_capture"], "not_attempted")
        self.assertFalse(receipt["container_smoke_completed"])
        self.assertIsNone(receipt["runtime_inventory_receipt"])
        self.assertIsNone(inventory)
        self.assertFalse(any("down" in call or "cp" in call for call in calls))


if __name__ == "__main__":
    unittest.main()
