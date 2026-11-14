#!/usr/bin/env python3
"""Qualify a stopped-state restore with retained backlog and new post-restore input.

Uses real, owned Kafka/Spark/MLflow processes on one host. The original deployment
and backup are preserved. This does not qualify a different host or Databricks.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import signal
import socket
import sys
import time
import urllib.request
from pathlib import Path

from industrial_mlops.config import PipelineConfig
from industrial_mlops.features import causal_feature_rows
from industrial_mlops.operations import backup_state, file_hash, restore_state, source_manifest, write_json
from industrial_mlops.store import Store, canonical
from benchmark_streaming import ResourceSampler
from qualify_native import Services, emit


class ObservedServices(Services):
    """Retain actual owned process identities after the shared helper pops them."""

    def __init__(self, work: Path, tools: Path):
        super().__init__(work, tools)
        self.owned = []

    def start(self, name, command):
        super().start(name, command)
        self.owned.append((name, self.children[name]))

    def wait_port(self, name, port, timeout=60):
        # A recorded broker startup reached RUNNING just after the original
        # 60-second listen deadline. Keep a declared 90-second broker allowance
        # inside the unchanged overall work/cleanup budget.
        super().wait_port(name, port, timeout=90 if name == "kafka" else timeout)

    def observe_groups(self) -> dict:
        import psutil
        groups = {process.pid for _, process in self.owned}
        members, errors = [], []
        for process in psutil.process_iter(["pid", "status"]):
            try:
                group = os.getpgid(process.pid)
                if group in groups:
                    members.append({"pid": process.pid, "group": group,
                                    "status": process.info["status"]})
            except (ProcessLookupError, psutil.NoSuchProcess):
                continue
            except PermissionError:
                errors.append({"pid": process.pid, "reason": "group_observation_denied"})
        roots = [{"name": name, "pid": process.pid, "returncode": process.poll()}
                 for name, process in self.owned]
        return {"roots": roots, "members": members, "observation_errors": errors,
                "live_members": [m for m in members if m["status"] not in ("zombie", "dead")]}

    def stop_and_observe(self) -> dict:
        close_errors = []
        try:
            self.close()
        except Exception as exc:
            # A helper failure must not skip cleanup of other owned processes.
            close_errors.append({"type": type(exc).__name__, "message": str(exc)})
        observation = self.observe_groups()
        # A completed Python wrapper can leave its JVM gateway in the owned group.
        for sig in (signal.SIGTERM, signal.SIGKILL):
            groups = {m["group"] for m in observation["live_members"]}
            groups.update(r["pid"] for r in observation["roots"] if r["returncode"] is None)
            for group in groups:
                try:
                    os.killpg(group, sig)
                except ProcessLookupError:
                    pass
            until = time.monotonic() + 5
            while groups and time.monotonic() < until:
                observation = self.observe_groups()
                if not observation["live_members"] and all(
                        r["returncode"] is not None for r in observation["roots"]):
                    break
                time.sleep(.1)
        observation = self.observe_groups()
        observation["close_errors"] = close_errors
        observation["no_live_owned_processes"] = (
            not close_errors and not observation["live_members"] and not observation["observation_errors"]
            and all(r["returncode"] is not None for r in observation["roots"]))
        observation["scope"] = "owned process groups and reaped roots; zombie entries are terminated, not live services"
        return observation


def directory_hashes(root: Path) -> dict:
    return {str(p.relative_to(root)): file_hash(p) for p in sorted(root.rglob("*")) if p.is_file()}


def ports_closed(ports: tuple[int, ...]) -> dict:
    result = {}
    for port in ports:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.5):
                result[str(port)] = False
        except ConnectionRefusedError:
            result[str(port)] = True
        except OSError as exc:
            result[str(port)] = {"unknown": type(exc).__name__}
    return result


def immutable_rows(store: Store) -> dict:
    """Capture committed source/business rows; runtime health is allowed to change."""
    keys = {
        "raw_records": ("topic", "partition_id", "offset_id"),
        "features": ("feature_id",), "predictions": ("feature_id",),
        "alerts": ("alert_id",), "batches": ("query_id", "batch_id"),
    }
    return {
        table: {
            canonical([row[key] for key in columns]): canonical(dict(row))
            for row in store.db.execute(f"SELECT * FROM {table}")
        }
        for table, columns in keys.items()
    }


def fingerprints(rows: dict) -> dict:
    return {table: {"rows": len(values), "sha256": hashlib.sha256(
        canonical(values).encode()).hexdigest()} for table, values in rows.items()}


def preserves(previous: dict, current: dict) -> bool:
    return all(current[table].get(key) == body
               for table, values in previous.items() for key, body in values.items())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--tools", default=".tools")
    parser.add_argument("--machines", type=int, default=6)
    parser.add_argument("--budget-seconds", type=int, default=600)
    args = parser.parse_args()
    if not 2 <= args.machines <= 8 or not 360 <= args.budget_seconds <= 900:
        raise ValueError("bounded profile requires 2..8 machines and 360..900 seconds")
    work, tools = Path(args.work_dir).resolve(), Path(args.tools).resolve()
    backup, restored = work.parent / f"{work.name}-backup", work.parent / f"{work.name}-restored"
    if any(path.exists() for path in (work, backup, restored)):
        raise ValueError("work, backup and restored paths must all be new")
    if not (tools / "jdk17" / "bin" / "java").is_file():
        raise ValueError("bootstrap the pinned native tools first")
    for port in (5000, 8080, 19092, 19093):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    (work / "evidence").mkdir(parents=True)
    (work / "pipeline").mkdir()
    config, source = PipelineConfig(), source_manifest()
    started = time.monotonic()
    deadline = started + args.budget_seconds - 180
    report = {
        "schema_version": "1", "scope": "real synthetic single-host stopped-state restore",
        "started_at": time.time(), "source": source, "runner_sha256": file_hash(Path(__file__)),
        "services_helper_sha256": file_hash(Path(__file__).with_name("qualify_native.py")),
        "resource_sampler_sha256": file_hash(Path(__file__).with_name("benchmark_streaming.py")),
        "python": platform.python_version(), "platform": platform.platform(),
        "runtime": {name: importlib.metadata.version(name) for name in (
            "pyspark", "mlflow", "kafka-python", "scikit-learn", "numpy", "pandas", "psutil")},
        "protocol": {"machines": args.machines, "sites": 2, "training_hours_per_split": 72,
                     "initial_hours": 24, "retained_backlog_hours": 2, "fresh_input_hours": 2,
                     "seeds": [42, 45, 46], "partitions": 3, "local_threads": 2,
                     "max_offsets_per_trigger": 200, "total_budget_seconds": args.budget_seconds,
                     "broker_listen_timeout_seconds": 90,
                     "mlflow_background_job_execution": False,
                     "cleanup_reserve_seconds": 180, "config": config.to_dict()},
        "checks": {}, "publications": [], "streams": {}, "resources": {},
        "business_protocol_complete": False,
        "provider": "not_executed", "cross_host_restore": "not_executed",
    }
    services, restored_services = ObservedServices(work, tools), None
    report["service_profile"] = {
        "mlflow_background_job_execution": services.env.get("MLFLOW_SERVER_ENABLE_JOB_EXECUTION"),
        "mlflow_http_workers": 1,
        "scope": "Owned local tracking/artifact/registry server; unused background job execution is disabled",
    }
    write_json(work / "evidence" / "protocol.json", report)
    uri, bootstrap, topic, query = "http://127.0.0.1:5000", "127.0.0.1:19092", "restore-backlog", "restore-backlog"
    db, checkpoint = work / "pipeline" / "monitor.db", work / "checkpoints"
    all_events = []

    def remaining() -> int:
        seconds = int(deadline - time.monotonic())
        if seconds < 1:
            raise TimeoutError("restoration work deadline reached; cleanup budget reserved")
        return seconds

    def alarm_handler(_signum, _frame):
        raise TimeoutError("restoration work deadline reached; cleanup budget reserved")

    previous_handler = signal.signal(signal.SIGALRM, alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, remaining())

    def publish(name: str, seed: int, hours: int, start: str) -> dict:
        from industrial_mlops.simulator import generate_dataset
        from industrial_mlops.streaming import produce
        remaining()
        dataset = generate_dataset(seed=seed, machines=args.machines, sites=2,
                                   hours=hours, start=start, anomalies=True)
        path = work / "inputs" / f"{name}.jsonl"
        path.parent.mkdir(exist_ok=True)
        path.write_text("\n".join(e if isinstance(e, str) else canonical(e)
                                  for e in dataset["events"]) + "\n", encoding="utf-8")
        receipt = produce(str(path), bootstrap, topic, partitions=3,
                          timeout_seconds=min(60, remaining()))
        publication = {"name": name, "input_sha256": file_hash(path),
                       "manifest": dataset["manifest"], "receipt": receipt}
        all_events.extend(dataset["events"])
        report["publications"].append(publication)
        write_json(work / "evidence" / f"{name}-producer.json", publication)
        return publication

    def consume(owner: Services, name: str, path: Path, cp: Path) -> dict:
        receipt = work / "evidence" / f"{name}-stream.json"
        duration = min(180, max(1, remaining() - 20))
        sampler = ResourceSampler(owner, name, work / "evidence" / f"{name}-resources.jsonl")
        sampler.start()
        try:
            owner.worker(name, [
                "--bootstrap", bootstrap, "--topic", topic, "--db", str(path),
                "--checkpoint", str(cp), "--tracking-uri", uri, "--query-id", query,
                "--available-now", "--duration-seconds", str(duration),
                "--max-offsets", "200", "--workers", "2", "--report", str(receipt),
            ], timeout=min(duration + 20, remaining()))
        finally:
            observation = sampler.stop()
            observation["thread_stopped"] = not sampler.thread.is_alive()
            observation["scope"]["window"] = "worker launch through completion/failure; training is a prior process"
            report["resources"][name] = observation
            write_json(work / "evidence" / f"{name}-resource-summary.json", observation)
            if not observation["thread_stopped"] or observation["sampler_errors"]:
                raise RuntimeError(f"Resource capture did not stop cleanly for {name}")
        result = json.loads(receipt.read_text())
        report["streams"][name] = result
        if not result["drained"] or not result["verification"]["valid"]:
            raise RuntimeError(f"{name} did not drain with valid sink invariants")
        return result

    def start_kafka(owner: Services, name: str, *, initialize=False) -> None:
        sampler = ResourceSampler(owner, "__startup_only__", work / "evidence" / f"{name}-resources.jsonl")
        sampler.start()
        try:
            owner.kafka(initialize=initialize)
        finally:
            observation = sampler.stop()
            observation["thread_stopped"] = not sampler.thread.is_alive()
            observation["scope"]["window"] = "broker format/startup through metadata readiness or failure"
            observation["scope"]["processes"] = "runner and registered owned service trees; the short-lived format subprocess is not sampled"
            report["resources"][name] = observation
            write_json(work / "evidence" / f"{name}-resource-summary.json", observation)
            if not observation["thread_stopped"] or observation["sampler_errors"]:
                raise RuntimeError(f"Resource capture did not stop cleanly for {name}")

    try:
        from industrial_mlops.registry import RegistryClient
        from industrial_mlops.streaming import source_lag
        report["checks"]["unused_mlflow_background_jobs_disabled"] = (
            report["service_profile"]["mlflow_background_job_execution"] == "false")
        if not report["checks"]["unused_mlflow_background_jobs_disabled"]:
            raise ValueError("The bounded local profile must disable unused MLflow background jobs")
        emit("restore_starting_owned_mlflow")
        services.mlflow()
        emit("restore_training_fixed_protocol")
        # Numerical training owns a separate process so its allocated memory is
        # released before the concurrent broker/Spark/registry streaming phase.
        services.start("training", [sys.executable, "-m", "industrial_mlops", "train",
                                    "--tracking-uri", uri, "--output", str(work / "training"),
                                    "--machines", str(args.machines), "--hours", "72"])
        training_returncode = services.children["training"].wait(timeout=min(180, remaining()))
        if training_returncode:
            raise RuntimeError(f"training process exited {training_returncode}; see training.log")
        training = json.loads((work / "training" / "training-report.json").read_text())
        model = RegistryClient(uri).load(str(training["models"]["candidate"]["model_version"]))
        report["model"] = {"model_name": model.model_name, "model_version": model.model_version,
                           "run_id": model.run_id, "qualified": model.qualified,
                           "threshold": model.threshold, "model_digest": model.model_digest,
                           "training_report_sha256": file_hash(work / "training" / "training-report.json")}
        with Store(db, config) as store:
            store.activate(model, shadow=not model.qualified)
        # Kafka is not used during training. Start it only for the streaming
        # phase instead of keeping an idle broker live through numerical work.
        emit("restore_starting_owned_kafka_after_training")
        start_kafka(services, "initial-broker-startup", initialize=True)
        publish("initial", 42, 24, "2026-07-01T00:00:00Z")
        initial_stream = consume(services, "initial", db, checkpoint)
        with Store(db, config, readonly=True) as store:
            initial_counts, initial_rows = store.counts(), immutable_rows(store)
            report["initial"] = {"counts": initial_counts, "fingerprints": fingerprints(initial_rows)}
            report["checks"]["initial_nonzero_predictions_and_alerts"] = (
                initial_counts["predictions"] > 0 and initial_counts["alerts"] > 0)
        backlog = publish("retained-backlog", 45, 2, "2026-07-03T00:00:00Z")
        with Store(db, config, readonly=True) as store:
            lag = source_lag(store, bootstrap, topic)
            report["pre_backup_lag"] = lag
            report["checks"]["nonempty_unconsumed_backlog_before_backup"] = (
                store.counts() == initial_counts and lag.get("status") == "observed"
                and lag.get("offsets_behind") == backlog["receipt"]["acknowledged"] > 0)
        report["original_stop"] = services.stop_and_observe()
        report["original_ports_closed"] = ports_closed((5000, 19092, 19093))
        report["checks"]["original_services_observed_stopped_before_backup"] = (
            report["original_stop"]["no_live_owned_processes"]
            and all(value is True for value in report["original_ports_closed"].values()))
        if not report["checks"]["original_services_observed_stopped_before_backup"]:
            raise RuntimeError("original service termination was not observed; backup refused")
        emit("restore_copying_stopped_state_with_backlog")
        original_binding_hash = file_hash(checkpoint / "pipeline-binding.json")
        original_checkpoint_hashes = directory_hashes(checkpoint)
        report["original_checkpoint_files"] = original_checkpoint_hashes
        report["backup"] = backup_state(work, backup, quiesced=True)
        report["restore"] = restore_state(backup, restored)
        restored_db, restored_cp = restored / "pipeline" / "monitor.db", restored / "checkpoints"
        with Store(restored_db, config, readonly=True) as store:
            report["checks"]["copied_sink_matches_original"] = (
                store.verify()["valid"] and immutable_rows(store) == initial_rows)
        binding_path = restored_cp / "pipeline-binding.json"
        original_binding = json.loads(binding_path.read_text())
        if original_binding["db_path"] != str(db):
            raise ValueError("copied checkpoint is not bound to this original sink")
        relocated = dict(original_binding, db_path=str(restored_db))
        write_json(binding_path, relocated)
        report["binding_relocation"] = {"before": original_binding, "after": relocated,
                                        "original_sha256": original_binding_hash}
        restored_services = ObservedServices(restored, tools)
        start_kafka(restored_services, "restored-broker-startup")
        restored_services.mlflow()
        restored_model = RegistryClient(uri).load(str(model.model_version))
        report["checks"]["restored_numeric_model_identity"] = all(
            str(getattr(model, name)) == str(getattr(restored_model, name))
            for name in ("model_name", "model_version", "run_id", "threshold", "feature_signature", "model_digest"))
        with Store(restored_db, config, readonly=True) as store:
            report["restored_pre_consume_lag"] = source_lag(store, bootstrap, topic)
        report["checks"]["retained_backlog_survives_broker_restore"] = (
            report["restored_pre_consume_lag"] == report["pre_backup_lag"])
        emit("restore_consuming_retained_backlog")
        backlog_stream = consume(restored_services, "restored-backlog", restored_db, restored_cp)
        report["checks"]["copied_spark_query_identity_preserved"] = (
            backlog_stream["query_id"] == initial_stream["query_id"])
        report["checks"]["restored_query_reads_only_unconsumed_backlog"] = (
            sum(p.get("numInputRows", 0) for p in backlog_stream["progress"])
            == backlog["receipt"]["acknowledged"])
        with Store(restored_db, config, readonly=True) as store:
            backlog_counts, backlog_rows = store.counts(), immutable_rows(store)
            lag = source_lag(store, bootstrap, topic)
            report["after_backlog"] = {"counts": backlog_counts,
                                       "fingerprints": fingerprints(backlog_rows), "source_lag": lag}
            report["checks"]["restored_backlog_consumed_and_scored"] = (
                backlog_counts["raw"] - initial_counts["raw"] == backlog["receipt"]["acknowledged"]
                and backlog_counts["predictions"] > initial_counts["predictions"]
                and backlog_counts["pending_predictions"] == 0)
            report["checks"]["old_business_rows_preserved_after_backlog"] = preserves(initial_rows, backlog_rows)
            report["checks"]["backlog_drained_to_observed_zero_lag"] = (
                lag.get("status") == "observed" and lag.get("offsets_behind") == 0)
        fresh = publish("post-restore", 46, 2, "2026-07-05T00:00:00Z")
        emit("restore_consuming_new_post_restore_input")
        fresh_stream = consume(restored_services, "post-restore", restored_db, restored_cp)
        report["checks"]["same_query_reads_only_fresh_post_restore_input"] = (
            fresh_stream["query_id"] == initial_stream["query_id"]
            and sum(p.get("numInputRows", 0) for p in fresh_stream["progress"])
            == fresh["receipt"]["acknowledged"])
        with Store(restored_db, config, readonly=True) as store:
            final_counts, final_rows = store.counts(), immutable_rows(store)
            report["final_verification"] = store.verify()
            report["checks"]["fresh_post_restore_input_consumed_and_scored"] = (
                final_counts["raw"] - backlog_counts["raw"] == fresh["receipt"]["acknowledged"]
                and final_counts["predictions"] > backlog_counts["predictions"]
                and final_counts["pending_predictions"] == 0)
            report["checks"]["old_business_rows_preserved_after_fresh_input"] = preserves(backlog_rows, final_rows)
            expected_raw = sum(p["receipt"]["acknowledged"] for p in report["publications"])
            report["checks"]["all_source_receipts_balance"] = final_counts["raw"] == expected_raw
            actual_features = {row["feature_id"]: json.loads(row["body"])
                               for row in store.db.execute("SELECT feature_id,body FROM features")}
            expected_features = {f["feature_id"]: f for f in causal_feature_rows(all_events, config)}
            predictions = [dict(row) for row in store.db.execute("SELECT * FROM predictions")]
            report["checks"]["restored_batch_stream_feature_parity"] = actual_features == expected_features
            report["checks"]["restored_batch_stream_prediction_parity"] = (
                len(predictions) == len(actual_features) and all(
                    abs(row["score"] - restored_model.predict(actual_features[row["feature_id"]])) < 1e-12
                    for row in predictions))
            report["checks"]["all_predictions_use_restored_version"] = all(
                row["model_version"] == str(restored_model.model_version)
                and row["run_id"] == restored_model.run_id for row in predictions)
            report["final_fingerprints"] = fingerprints(final_rows)
            write_json(work / "evidence" / "dashboard-snapshot.json", store.snapshot())
        replay = consume(restored_services, "empty-resume", restored_db, restored_cp)
        report["checks"]["one_query_identity_across_distinct_worker_runs"] = (
            len({r["query_id"] for r in report["streams"].values()}) == 1
            and len({r["run_id"] for r in report["streams"].values()}) == len(report["streams"]))
        with Store(restored_db, config, readonly=True) as store:
            final_lag = source_lag(store, bootstrap, topic)
            report["final_source_lag"] = final_lag
            report["checks"]["repeated_checkpoint_resume_has_no_duplicate_business_effects"] = (
                immutable_rows(store) == final_rows and store.counts() == final_counts
                and sum(p.get("numInputRows", 0) for p in replay["progress"]) == 0)
            report["checks"]["final_source_lag_zero"] = (
                final_lag.get("status") == "observed" and final_lag.get("offsets_behind") == 0)
        with Store(db, config, readonly=True) as store:
            report["checks"]["original_sink_and_checkpoint_preserved"] = (
                immutable_rows(store) == initial_rows
                and directory_hashes(checkpoint) == original_checkpoint_hashes)
        restored_services.start("dashboard", [sys.executable, "-m", "industrial_mlops", "dashboard",
                                             "--db", str(restored_db), "--port", "8080"])
        restored_services.wait_port("dashboard", 8080)
        with urllib.request.urlopen("http://127.0.0.1:8080/api/snapshot", timeout=5) as response:
            snapshot = json.load(response)
        with urllib.request.urlopen("http://127.0.0.1:8080/", timeout=5) as response:
            (work / "evidence" / "dashboard-response.html").write_bytes(response.read())
        write_json(work / "evidence" / "dashboard-snapshot.json", snapshot)
        report["checks"]["dashboard_reads_restored_live_sink"] = snapshot["counts"] == final_counts
        report["checks"]["source_unchanged_during_run"] = source_manifest()["files"] == source["files"]
        report["business_protocol_complete"] = True
        emit("restore_business_checks_complete", checks=report["checks"])
    except BaseException as exc:
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)[-5000:]}
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        owners = [owner for owner in (restored_services, services) if owner is not None]
        cleanup_errors, observations = [], []
        for owner in owners:
            try:
                observations.append(owner.stop_and_observe())
            except Exception as exc:
                cleanup_errors.append({"type": type(exc).__name__, "message": str(exc)})
        report["cleanup"] = {"errors": cleanup_errors,
                             "observations": observations,
                             "ports_closed": ports_closed((5000, 8080, 19092, 19093))}
        report["finished_at"] = time.time()
        report["elapsed_seconds"] = time.monotonic() - started
        report["checks"]["owned_services_stopped"] = (
            not cleanup_errors and len(observations) == len(owners)
            and all(o["no_live_owned_processes"] for o in observations)
            and all(value is True for value in report["cleanup"]["ports_closed"].values()))
        report["checks"]["within_declared_total_budget"] = report["elapsed_seconds"] <= args.budget_seconds
        report["passed"] = (
            report["business_protocol_complete"] and bool(report["checks"])
            and all(report["checks"].values()) and "failure" not in report)
        write_json(work / "evidence" / "restore-qualification.json", report)
    if not report["passed"]:
        raise RuntimeError("restoration qualification failed; original evidence retained")
    emit("restore_qualification_complete", passed=report["passed"], checks=len(report["checks"]))


if __name__ == "__main__":
    main()
