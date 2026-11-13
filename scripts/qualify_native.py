#!/usr/bin/env python3
"""Exercise real local Kafka, Spark and MLflow in one isolated process namespace.

This is not a Databricks or multi-node benchmark. Processes created here are
terminated in finally; data/evidence remain available for review.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import signal
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from industrial_mlops.config import PipelineConfig
from industrial_mlops.features import causal_feature_rows
from industrial_mlops.operations import backup_state, file_hash, restore_state, source_manifest, write_json
from industrial_mlops.store import Store


class Services:
    def __init__(self, work: Path, tools: Path):
        self.work, self.tools = work, tools
        self.children = {}
        self.handles = []
        self.env = dict(os.environ)
        self.env.update({
            "JAVA_HOME": str(tools / "jdk17"), "SPARK_LOCAL_IP": "127.0.0.1",
            "PYSPARK_PYTHON": sys.executable, "PYSPARK_DRIVER_PYTHON": sys.executable,
            "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
            "MLFLOW_DISABLE_AGENT_HINT": "1", "MLFLOW_HTTP_REQUEST_TIMEOUT": "5",
            "MLFLOW_DISABLE_TELEMETRY": "true", "DO_NOT_TRACK": "true",
            "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "0", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
            "SPARK_KAFKA_JARS": str(tools / "spark-jars"),
        })
        os.environ.update(self.env)

    def start(self, name, command):
        log = (self.work / "evidence" / f"{name}.log").open("a", encoding="utf-8")
        self.handles.append(log)
        self.children[name] = subprocess.Popen(
            command, env=self.env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )

    def stop(self, name, *, hard=False):
        child = self.children.pop(name, None)
        if child is None or child.poll() is not None:
            return
        os.killpg(child.pid, signal.SIGKILL if hard else signal.SIGTERM)
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=10)

    def close(self):
        for name in list(self.children):
            self.stop(name)
        for handle in self.handles:
            handle.close()

    def worker(self, name, command, *, timeout=420):
        """A deployment-style fresh Python/JVM worker for each lifecycle run."""
        self.start(name, [sys.executable, "-m", "industrial_mlops", "stream", *command])
        child = self.children[name]
        try:
            returncode = child.wait(timeout=timeout)
            if returncode:
                raise RuntimeError(f"worker {name} exited {returncode}; see {name}.log")
        except subprocess.TimeoutExpired:
            self.stop(name, hard=True)
            raise
        finally:
            self.children.pop(name, None)
            # A stopped Python worker must not leave a gateway in its owned process group.
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def wait_port(self, name, port, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.children[name].poll() is not None:
                raise RuntimeError(f"{name} exited; inspect its original log")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(.25)
        raise TimeoutError(f"{name} did not become reachable")

    def mlflow(self):
        directory = self.work / "services" / "mlflow"
        (directory / "artifacts").mkdir(parents=True, exist_ok=True)
        self.start("mlflow", [
            sys.executable, "-m", "mlflow", "server", "--host", "127.0.0.1",
            "--port", "5000", "--workers", "1",
            "--backend-store-uri", f"sqlite:///{directory / 'mlflow.db'}",
            "--artifacts-destination", str(directory / "artifacts"),
            "--allowed-hosts", "127.0.0.1:5000,localhost:5000",
        ])
        self.wait_port("mlflow", 5000)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen("http://127.0.0.1:5000/health", timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                time.sleep(.25)
        raise TimeoutError("MLflow health did not become ready")

    def kafka(self, *, initialize=False):
        directory = self.work / "services" / "kafka"
        directory.mkdir(parents=True, exist_ok=True)
        properties = directory / "server.properties"
        properties.write_text("\n".join([
            "process.roles=broker,controller", "node.id=1",
            "controller.quorum.voters=1@127.0.0.1:19093",
            "listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093",
            "advertised.listeners=PLAINTEXT://127.0.0.1:19092",
            "inter.broker.listener.name=PLAINTEXT", "controller.listener.names=CONTROLLER",
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "num.network.threads=3", "num.io.threads=4", "num.partitions=3",
            "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1", "log.retention.hours=72",
            "log.retention.bytes=268435456", "log.segment.bytes=67108864",
            "auto.create.topics.enable=false", f"log.dirs={directory / 'logs'}", "",
        ]), encoding="utf-8")
        kafka = self.tools / "kafka_2.13-3.9.2" / "bin"
        if initialize:
            completed = subprocess.run(
                [str(kafka / "kafka-storage.sh"), "format", "-t", "Y-IGX9OARu2R0gYdVpWUEA",
                 "-c", str(properties)], env=self.env, capture_output=True, text=True, timeout=60,
            )
            (self.work / "evidence" / "kafka-format.log").write_text(
                completed.stdout + completed.stderr, encoding="utf-8")
            completed.check_returncode()
        self.start("kafka", [str(kafka / "kafka-server-start.sh"), str(properties)])
        self.wait_port("kafka", 19092)
        # A listening socket can precede usable controller metadata. Do not publish
        # until an actual broker request has completed within the bounded deadline.
        from kafka import KafkaAdminClient
        deadline, last_error = time.monotonic() + 60, None
        while time.monotonic() < deadline:
            if self.children["kafka"].poll() is not None:
                raise RuntimeError("Kafka exited before metadata readiness")
            admin = None
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers="127.0.0.1:19092", request_timeout_ms=5000,
                    api_version_auto_timeout_ms=5000,
                )
                cluster = admin.describe_cluster()
                if not cluster.get("brokers") or cluster.get("controller_id", -1) < 0:
                    raise RuntimeError("broker/controller metadata is not available")
                admin.list_topics()
                write_json(self.work / "evidence" / "kafka-ready.json", {
                    "checked_at": time.time(), "cluster": cluster,
                    "scope": "successful Kafka cluster and topic metadata requests",
                })
                return
            except Exception as exc:
                last_error = exc
                time.sleep(.5)
            finally:
                if admin is not None:
                    admin.close()
        raise TimeoutError(f"Kafka metadata did not become ready: {last_error}")


def emit(stage, **fields):
    print(json.dumps({"stage": stage, **fields}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--tools", default=".tools")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--training-hours", type=int, default=72)
    parser.add_argument("--machines", type=int, default=4)
    parser.add_argument("--reuse-training-from", help="reuse matching stopped MLflow state without retraining")
    args = parser.parse_args()
    work, tools = Path(args.work_dir).resolve(), Path(args.tools).resolve()
    if work.exists():
        raise ValueError("qualification requires a new directory; preserve earlier failed evidence")
    if not (tools / "jdk17" / "bin" / "java").is_file():
        raise ValueError("run scripts/bootstrap_native.sh first (or use the Docker reference)")
    (work / "evidence").mkdir(parents=True)
    (work / "pipeline").mkdir()
    if args.reuse_training_from:
        prior = Path(args.reuse_training_from).resolve()
        from industrial_mlops.modeling import training_source_manifest
        prior_report = json.loads((prior / "training" / "training-report.json").read_text())
        if prior_report["training_source"] != training_source_manifest():
            raise ValueError("training sources changed; prior model cannot be silently reused")
        shutil.copytree(prior / "services" / "mlflow", work / "services" / "mlflow")
        shutil.copytree(prior / "training", work / "training")
    config = PipelineConfig()
    source = source_manifest()
    write_json(work / "evidence" / "source-manifest.json", source)
    report = {
        "scope": "isolated single-host JVM Kafka + local Spark + HTTP MLflow; synthetic data only",
        "source": source, "started_at": time.time(),
        "runner_sha256": file_hash(Path(__file__)),
        "protocol": {"hours": args.hours, "machines": args.machines, "seed": 42,
                     "training_hours_per_split": args.training_hours, "config": config.to_dict(),
                     "gates_predeclared": True, "distributed_scale": False},
        "runtime": {name: importlib.metadata.version(name) for name in (
            "pyspark", "mlflow", "scikit-learn", "numpy", "pandas", "kafka-python")},
        "python": platform.python_version(), "platform": platform.platform(),
        "checks": {},
    }
    services = Services(work, tools)
    uri = "http://127.0.0.1:5000"
    db_path, checkpoint = work / "pipeline" / "monitor.db", work / "checkpoints"
    topic = "qualification"
    try:
        emit("starting_real_services")
        services.kafka(initialize=True)
        services.mlflow()
        from industrial_mlops.modeling import train_and_register
        from industrial_mlops.registry import RegistryClient
        from industrial_mlops.simulator import generate_dataset
        from industrial_mlops.streaming import produce

        if args.reuse_training_from:
            emit("reusing_registered_models_with_matching_training_source")
            training = prior_report
            report["training_reuse"] = {
                "prior_directory": str(prior), "original_code_identity": training["code_sha"],
                "training_source_unchanged": True, "new_training_run": False,
            }
        else:
            emit("training_with_fixed_protocol")
            training = train_and_register(uri, work / "training", config, source["identity"],
                                          machines=args.machines, hours=args.training_hours)
        report["training"] = training
        registry = RegistryClient(uri)
        versions = {}
        # Registration returns one entry per reviewed role; numeric identities come from MLflow.
        for role in ("baseline", "candidate"):
            info = training["models"][role]
            versions[role] = str(info["model_version"])
        baseline, candidate = registry.load(versions["baseline"]), registry.load(versions["candidate"])
        report["registered_versions"] = versions
        with Store(db_path, config) as store:
            store.activate(baseline, shadow=not baseline.qualified)
        all_events, publications = [], []

        def publish(seed, hours, start, name, partitions=3):
            dataset = generate_dataset(
                seed=seed, machines=args.machines, hours=hours, start=start, anomalies=True,
            )
            path = work / f"{name}.jsonl"
            path.write_text("\n".join(event if isinstance(event, str) else json.dumps(event, sort_keys=True)
                                      for event in dataset["events"]) + "\n", encoding="utf-8")
            receipt = produce(str(path), "127.0.0.1:19092", topic, partitions=partitions)
            all_events.extend(dataset["events"])
            publications.append({"name": name, "manifest": dataset["manifest"], "receipt": receipt})
            return dataset

        def consume(path=db_path, cp=checkpoint, query="qualification", **extra):
            number = len(list((work / "evidence").glob(query + "-*.log")))
            name = f"{query}-{number}"
            receipt = work / "evidence" / f"{name}.json"
            command = [
                "--bootstrap", "127.0.0.1:19092", "--topic", topic, "--db", str(path),
                "--checkpoint", str(cp), "--tracking-uri", uri, "--query-id", query,
                "--available-now", "--duration-seconds", "300", "--max-offsets", "1000",
                "--workers", "2", "--report", str(receipt),
            ]
            for key, value in extra.items():
                command.extend(["--" + key.replace("_", "-"), str(value)])
            services.worker(name, command)
            return json.loads(receipt.read_text())

        emit("publishing_telemetry_with_poison_late_and_duplicate_cases")
        publish(42, args.hours, "2026-05-01T00:00:00Z", "initial")
        services.stop("mlflow")
        emit("consuming_with_unavailable_model")
        consume()
        with Store(db_path, config, readonly=True) as store:
            counts = store.counts()
            report["checks"]["unavailable_model_retains_features"] = counts["features"] > 0 and counts["predictions"] == 0
        services.mlflow()
        emit("resuming_checkpoint_and_scoring_retained_features")
        consume()
        with Store(db_path, config, readonly=True) as store:
            report["checks"]["model_recovery_scores_pending"] = store.counts()["pending_predictions"] == 0
            initial_counts = store.counts()
            actual_features = {row["feature_id"]: json.loads(row["body"]) for row in store.db.execute("SELECT * FROM features")}
            reference = {row["feature_id"]: row for row in causal_feature_rows(all_events, config)}
            report["checks"]["batch_stream_feature_parity"] = actual_features == reference
            report["checks"]["batch_stream_prediction_parity"] = all(
                abs(row["score"] - baseline.predict(actual_features[row["feature_id"]])) < 1e-12
                for row in store.db.execute("SELECT feature_id,score FROM predictions")
            )
            report["checks"]["source_count_matches_producer"] = initial_counts["raw"] == publications[0]["receipt"]["acknowledged"]
        emit("testing_transaction_boundary_recovery")
        for point in ("before_commit", "after_commit"):
            failure_seen = False
            path, cp = work / "pipeline" / f"{point}.db", work / f"{point}-checkpoint"
            with Store(path, config) as store:
                store.activate(baseline, shadow=not baseline.qualified)
            try:
                consume(path, cp, point, failpoint=point, fail_batch=0,
                        fail_marker=str(work / f"{point}.marker"))
            except Exception as exc:
                failure_seen = True
                write_json(work / "evidence" / f"{point}-failure.json",
                           {"injected": point, "exception": type(exc).__name__, "message": str(exc)[-4000:]})
            consume(path, cp, point)
            with Store(path, config, readonly=True) as store:
                report["checks"][f"{point}_recovery"] = (
                    failure_seen and store.verify()["valid"]
                    and store.counts()["raw"] == initial_counts["raw"]
                    and store.counts()["features"] == initial_counts["features"]
                    and store.counts()["predictions"] == initial_counts["predictions"]
                    and store.counts()["alerts"] == initial_counts["alerts"])
        emit("killing_and_restarting_broker_during_active_query")
        ready_path = work / "active-query-ready.json"
        outage = {}

        def interrupt_broker():
            try:
                deadline = time.monotonic() + 60
                while not ready_path.exists() and time.monotonic() < deadline:
                    time.sleep(.2)
                if not ready_path.exists():
                    raise TimeoutError("active query did not start before broker exercise")
                time.sleep(2)
                outage["stopped_at"] = time.time()
                services.stop("kafka", hard=True)
                try:
                    with socket.create_connection(("127.0.0.1", 19092), timeout=1):
                        outage["port_closed"] = False
                except OSError:
                    outage["port_closed"] = True
                time.sleep(3)
                services.kafka()
                outage["restarted_at"] = time.time()
                publish(45, 2, "2026-05-03T00:00:00Z", "broker-recovery")
                outage["new_input_acknowledged"] = True
            except Exception as exc:
                outage["failure"] = {"type": type(exc).__name__, "message": str(exc)}

        interrupter = threading.Thread(target=interrupt_broker, daemon=True)
        interrupter.start()
        active_receipt = work / "evidence" / "active-query.json"
        services.worker("active-query", [
            "--bootstrap", "127.0.0.1:19092", "--topic", topic, "--db", str(db_path),
            "--checkpoint", str(checkpoint), "--tracking-uri", uri,
            "--query-id", "qualification", "--duration-seconds", "45",
            "--max-offsets", "1000", "--workers", "2", "--ready-file", str(ready_path),
            "--report", str(active_receipt),
        ], timeout=180)
        active_run = json.loads(active_receipt.read_text())
        interrupter.join(timeout=60)
        if interrupter.is_alive() or "failure" in outage:
            raise RuntimeError(f"broker interruption exercise did not finish: {outage}")
        write_json(work / "evidence" / "active-broker-outage.json", {"outage": outage, "stream": active_run})
        consume()
        with Store(db_path, config, readonly=True) as store:
            report["checks"]["active_broker_outage_recovery"] = bool(
                outage.get("port_closed") and outage.get("new_input_acknowledged")
                and store.counts()["raw"] == sum(p["receipt"]["acknowledged"] for p in publications)
                and store.verify()["valid"])
        emit("promoting_and_activating_candidate")
        if candidate.qualified:
            report["promotion"] = registry.promote(candidate.model_version)
        else:
            report["promotion"] = {"status": "blocked_by_recorded_gates", "shadow_only": True}
        with Store(db_path, config) as store:
            store.activate(candidate, shadow=not candidate.qualified)
        publish(43, 2, "2026-05-05T00:00:00Z", "candidate")
        consume()
        emit("rolling_back_and_proving_new_predictions")
        with Store(db_path, config) as store:
            previous = store.previous_deployment()
            restored_model = registry.load(previous["model_version"])
            store.activate(restored_model, shadow=previous["mode"] == "shadow", action="rollback")
        publish(44, 2, "2026-05-10T00:00:00Z", "rollback")
        consume()
        with Store(db_path, config, readonly=True) as store:
            by_version = {row["model_version"]: row["n"] for row in store.db.execute(
                "SELECT model_version,COUNT(*) n FROM predictions GROUP BY model_version")}
            report["checks"]["candidate_scored_real_stream"] = by_version.get(candidate.model_version, 0) > 0
            report["checks"]["rollback_scored_real_stream"] = (
                store.deployment()["action"] == "rollback" and
                by_version.get(baseline.model_version, 0) > initial_counts["predictions"])
            report["final_verification"] = store.verify()
            report["checks"]["final_invariants"] = report["final_verification"]["valid"]
            report["checks"]["all_source_receipts_balance"] = store.counts()["raw"] == sum(
                p["receipt"]["acknowledged"] for p in publications)
            snapshot = store.snapshot()
            write_json(work / "evidence" / "dashboard-snapshot.json", snapshot)
            latencies = [row[0] for row in store.db.execute("SELECT inference_ms FROM predictions")]
            end_to_end = [row[0] for row in store.db.execute(
                "SELECT arrival_to_prediction_ms FROM predictions WHERE arrival_to_prediction_ms IS NOT NULL")]
            import numpy as np
            report["observed_latencies_ms"] = {
                "protocol": "per-prediction timing including DataFrame conversion; e2e includes intentional registry outage",
                "inference": dict(zip(("p50", "p95", "p99"), np.quantile(latencies, [.5, .95, .99]).tolist())),
                "broker_to_prediction": dict(zip(("p50", "p95", "p99"), np.quantile(end_to_end, [.5, .95, .99]).tolist())),
            }
        from industrial_mlops.cli import execute, parser as cli_parser
        monitor_args = cli_parser().parse_args(["monitor", "--db", str(db_path), "--tracking-uri", uri])
        report["monitoring"] = execute(monitor_args, config)
        report["checks"]["drift_does_not_promote"] = report["monitoring"]["promotion_performed"] is False
        emit("verifying_live_dashboard")
        services.start("dashboard", [sys.executable, "-m", "industrial_mlops", "dashboard",
                                    "--db", str(db_path), "--port", "8080"])
        services.wait_port("dashboard", 8080)
        with urllib.request.urlopen("http://127.0.0.1:8080/api/snapshot", timeout=5) as response:
            live = json.load(response)
        report["checks"]["dashboard_reads_real_sink"] = live["counts"] == report["final_verification"]["counts"]
        with urllib.request.urlopen("http://127.0.0.1:8080/", timeout=5) as response:
            (work / "evidence" / "dashboard-response.html").write_bytes(response.read())
        services.stop("dashboard")
        report["publications"] = publications
        services.stop("kafka")
        services.stop("mlflow")
        emit("testing_stopped_state_restore")
        # Full stopped deployment, including broker log, registry artifacts and checkpoints.
        backup = work.parent / f"{work.name}-backup"
        restored = work.parent / f"{work.name}-restored"
        backup_state(work, backup, quiesced=True)
        restore_state(backup, restored)
        restored_db = restored / "pipeline" / "monitor.db"
        with Store(restored_db, config, readonly=True) as store:
            report["checks"]["pipeline_database_restore"] = (
                store.verify()["valid"] and store.counts() == report["final_verification"]["counts"])
        restored_services = Services(restored, tools)
        try:
            restored_services.kafka()
            restored_services.mlflow()
            restored_registry = RegistryClient(uri)
            restored_baseline = restored_registry.load(baseline.model_version)
            sample = next(iter(actual_features.values()))
            report["checks"]["mlflow_artifact_restore"] = (
                abs(restored_baseline.predict(sample) - baseline.predict(sample)) < 1e-12)
            from kafka import KafkaConsumer, TopicPartition
            consumer = KafkaConsumer(bootstrap_servers="127.0.0.1:19092", group_id=None)
            try:
                partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
                offsets = consumer.end_offsets(partitions)
                report["checks"]["kafka_log_restore"] = sum(offsets.values()) == sum(
                    p["receipt"]["acknowledged"] for p in publications)
            finally:
                consumer.close()
            # Explicit relocation of the verified copy; original binding remains intact.
            binding_path = restored / "checkpoints" / "pipeline-binding.json"
            binding = json.loads(binding_path.read_text())
            if binding["db_path"] != str(db_path):
                raise ValueError("unexpected original binding during restore relocation")
            binding["db_path"] = str(restored_db)
            write_json(binding_path, binding)
            restored_receipt = work / "evidence" / "restored-checkpoint-run.json"
            restored_services.worker("restored-worker", [
                "--bootstrap", "127.0.0.1:19092", "--topic", topic, "--db", str(restored_db),
                "--checkpoint", str(restored / "checkpoints"), "--tracking-uri", uri,
                "--query-id", "qualification", "--available-now", "--duration-seconds", "120",
                "--max-offsets", "1000", "--workers", "2", "--report", str(restored_receipt),
            ], timeout=210)
            with Store(restored_db, config, readonly=True) as store:
                report["checks"]["restored_checkpoint_resume"] = (
                    store.counts() == report["final_verification"]["counts"])
        finally:
            restored_services.close()
        report["restore_scope"] = (
            "stopped full deployment; SQLite reopened, Kafka offsets observed, MLflow model reloaded "
            "and prediction checked, checkpoint explicitly relocated and resumed without duplicate effects")
        report["finished_at"] = time.time()
        report["elapsed_seconds"] = report["finished_at"] - report["started_at"]
        report["checks"]["source_unchanged_during_run"] = source_manifest()["files"] == source["files"]
        report["passed"] = all(report["checks"].values())
        report["databricks"] = "not_executed_no_authorized_workspace"
        report["billing"] = "No provider billing data; isolated local CPU execution is not a cloud cost estimate."
        write_json(work / "evidence" / "qualification.json", report)
        emit("qualification_complete", passed=report["passed"], checks=report["checks"])
        if not report["passed"]:
            raise RuntimeError("one or more integration assertions failed; see original evidence")
    except Exception as exc:
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)[-5000:]}
        report["passed"] = False
        write_json(work / "evidence" / "qualification.json", report)
        raise
    finally:
        services.close()


if __name__ == "__main__":
    main()
