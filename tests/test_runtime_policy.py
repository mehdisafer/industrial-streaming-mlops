"""SDK and local tracking-server policies stay explicit and scoped."""

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import yaml


ROOT = Path(__file__).resolve().parents[1]
JOB_EXECUTION_FLAG = "MLFLOW_SERVER_ENABLE_JOB_EXECUTION"


def test_project_import_disables_mlflow_telemetry_before_sdk_initialization():
    environment = dict(os.environ)
    for key in ("MLFLOW_DISABLE_TELEMETRY", "DO_NOT_TRACK"):
        environment.pop(key, None)
    command = (
        "import industrial_mlops; "
        "from mlflow.telemetry import get_telemetry_client; "
        "assert get_telemetry_client() is None"
    )
    subprocess.run([sys.executable, "-c", command], env=environment,
                   check=True, capture_output=True, text=True, timeout=30)


def test_native_tracking_profile_disables_unused_server_jobs_in_isolated_environment(tmp_path):
    parent_value = os.environ.get(JOB_EXECUTION_FLAG)
    environment = dict(os.environ)
    environment[JOB_EXECUTION_FLAG] = "true"
    command = textwrap.dedent("""
        import importlib.util
        import json
        from pathlib import Path
        import sys

        def forbid_network(event, args):
            if event in {"socket.connect", "socket.getaddrinfo", "socket.bind"}:
                raise AssertionError(f"unexpected network action: {event}")

        sys.addaudithook(forbid_network)
        import industrial_mlops
        from mlflow.environment_variables import MLFLOW_SERVER_ENABLE_JOB_EXECUTION

        # SDK import may inspect the OS via uname; Services must spawn nothing here.
        def forbid_process(event, args):
            if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
                raise AssertionError(f"unexpected service process: {event}")

        sys.addaudithook(forbid_process)
        # Importing the application must not alter an external server's job policy.
        assert MLFLOW_SERVER_ENABLE_JOB_EXECUTION.get() is True
        root, work = Path(sys.argv[1]), Path(sys.argv[2])
        spec = importlib.util.spec_from_file_location(
            "native_policy_check", root / "scripts" / "qualify_native.py")
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        services = native.Services(work, work / "tools")
        assert services.env["MLFLOW_SERVER_ENABLE_JOB_EXECUTION"] == "false"
        resolved = MLFLOW_SERVER_ENABLE_JOB_EXECUTION.get()
        assert resolved is False
        assert not services.children and not services.handles
        print(json.dumps({"job_execution": resolved, "services_started": 0}))
    """)
    completed = subprocess.run(
        [sys.executable, "-c", command, str(ROOT), str(tmp_path)], cwd=ROOT,
        env=environment, check=True, capture_output=True, text=True, timeout=30,
    )
    assert json.loads(completed.stdout) == {"job_execution": False, "services_started": 0}
    assert os.environ.get(JOB_EXECUTION_FLAG) == parent_value


def test_compose_disables_server_jobs_only_for_mlflow_and_preserves_shared_environment():
    config = yaml.safe_load((ROOT / "compose.yaml").read_text())
    common = config["x-app"]["environment"]
    assert JOB_EXECUTION_FLAG not in common
    assert config["services"]["mlflow"]["environment"] == {
        **common, JOB_EXECUTION_FLAG: "false",
    }
    for name, service in config["services"].items():
        if name != "mlflow":
            assert JOB_EXECUTION_FLAG not in service.get("environment", {})
