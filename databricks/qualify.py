"""Read-only provider preflight and run receipt collection via an installed CLI.

Deployment and run launch remain explicit bundle commands. This script does not
create compute, change permissions, install libraries, or start a stopped cluster.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess


def cli(arguments: list[str], profile: str | None) -> dict:
    command = ["databricks", *arguments, "--output", "json"]
    if profile:
        command.extend(["--profile", profile])
    result = subprocess.run(command, text=True, capture_output=True, timeout=60)
    if result.returncode:
        # CLI diagnostics can contain workspace or credential configuration.
        raise RuntimeError(f"Databricks read failed for {' '.join(arguments[:2])}; inspect CLI output locally without publishing credentials")
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile")
    subcommands = parser.add_subparsers(dest="command", required=True)
    preflight = subcommands.add_parser("preflight")
    preflight.add_argument("--cluster-id", required=True)
    preflight.add_argument("--output", type=Path, required=True)
    collect = subcommands.add_parser("collect")
    collect.add_argument("--run-id", type=int, required=True)
    collect.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "preflight":
        if not re.fullmatch(r"[A-Za-z0-9-]{1,128}", args.cluster_id):
            raise ValueError("Invalid existing compute identifier")
        cluster = cli(["clusters", "get", args.cluster_id], args.profile)
        identity = cli(["current-user", "me"], args.profile)
        permissions = cli(["permissions", "get", "clusters", args.cluster_id], args.profile)
        observation = {
            "operation": "read_only_preflight",
            "cluster_id": cluster["cluster_id"], "runtime": cluster.get("spark_version"),
            "state": cluster.get("state"), "access_mode": cluster.get("data_security_mode"),
            "identity_id": identity.get("id"),
            "permission_levels_observed": sorted({entry["permission_level"] for grant in permissions.get("access_control_list", []) for entry in grant.get("all_permissions", [])}),
            "permission_limitation": "Observed ACL levels are not a determination of this identity's effective group permissions.",
            "compute_ready": cluster.get("state") == "RUNNING",
            "status": "observed_pending_operator_review",
            "unverified": ["effective attach/run/library permissions", "Kafka network and ACL", "model access and compatibility", "catalog/schema/volume access", "provider dependency resolution", "consumption budget"],
        }
    else:
        if args.run_id <= 0:
            raise ValueError("A positive provider run ID is required")
        run = cli(["jobs", "get-run", str(args.run_id)], args.profile)
        observation = {
            "operation": "read_only_run_receipt", "run_id": run["run_id"], "job_id": run.get("job_id"),
            "run_page_url": run.get("run_page_url"), "state": run.get("state"), "status": run.get("status"),
            "start_time": run.get("start_time"), "end_time": run.get("end_time"),
            "setup_duration": run.get("setup_duration"), "execution_duration": run.get("execution_duration"),
            "cleanup_duration": run.get("cleanup_duration"),
            "tasks": [{"task_key": task.get("task_key"), "run_id": task.get("run_id"), "state": task.get("state"),
                       "cluster_instance": task.get("cluster_instance")} for task in run.get("tasks", [])],
            "consumption": None,
            "consumption_status": "not_observed_requires_authorized_billing_source",
            "evidence_requirement": "Retrieve run-<run_id>.json from the authorized output volume and reconcile counts, model identity and table versions. A terminal SUCCESS alone does not accept the project.",
        }
    observation["observed_at"] = datetime.now(timezone.utc).isoformat()
    observation["cli_version"] = subprocess.check_output(["databricks", "version"], text=True, timeout=15).strip()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(observation, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "operation": observation["operation"]}))


if __name__ == "__main__":
    main()
