"""Industrial maintenance reference. No client or personal-machine data."""

import os

# Project clients opt out before any MLflow import. Service launch profiles also
# set this explicitly so server/UI and child workers inherit the same policy.
os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")

__version__ = "0.1.0"
