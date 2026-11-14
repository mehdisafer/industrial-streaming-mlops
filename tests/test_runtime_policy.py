"""The project's SDK imports must not create an external telemetry client."""

import os
import subprocess
import sys


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
