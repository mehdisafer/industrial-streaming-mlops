#!/usr/bin/env python3
"""Bounded real Kafka/Spark/MLflow comparison on one Linux host.

Run only after the qualification services have stopped. This reuses an unchanged
training snapshot; it never trains, promotes, or creates cloud resources. Each
variant uses a fresh CLI/JVM through qualify_native.Services.worker().
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import psutil

from industrial_mlops.config import PipelineConfig
from industrial_mlops.operations import file_hash, source_manifest, write_json
from industrial_mlops.store import Store, canonical
from qualify_native import Services


ROOT = Path(__file__).resolve().parents[1]
CASES = (
    {"name": "h02-p1-w1", "hours": 2, "partitions": 1, "workers": 1},
    {"name": "h08-p1-w1", "hours": 8, "partitions": 1, "workers": 1},
    {"name": "h08-p3-w1", "hours": 8, "partitions": 3, "workers": 1},
    {"name": "h08-p3-w2", "hours": 8, "partitions": 3, "workers": 2},
)
BOOTSTRAP = "127.0.0.1:19092"
TRACKING_URI = "http://127.0.0.1:5000"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def emit(stage, **fields):
    print(canonical({"stage": stage, **fields}), flush=True)


def protocol(config, shadow=False):
    return {
        "schema_version": 1,
        "scope": "one host; real JVM Kafka, local Spark, HTTP MLflow and SQLite sink; synthetic data",
        "variants": list(CASES), "machines": 4, "sites": 2, "seed": 42,
        "start": "2026-06-01T00:00:00Z", "anomalies": True,
        "input_reuse": "all three 8h variants use the identical JSONL bytes",
        "max_offsets_per_trigger": 200, "max_worker_wall_seconds": 300,
        "total_wall_budget_seconds": 600, "work_budget_seconds": 580,
        "cleanup_budget_reserved_seconds": 20,
        "activation": "explicit_shadow" if shadow else "existing_qualified_candidate",
        "training": "copy stopped MLflow and training state; compare hashes and training source; no retraining or promotion",
        "config": config.to_dict(), "score_comparison_absolute_tolerance": 1e-12,
        "acceptance": [
            "source count equals producer acknowledgements", "persisted invariants all true",
            "availableNow drains and final observed source lag is zero",
            "all finalized features have predictions from the selected numerical model version",
            "8h variants have identical canonical features and scores within 1e-12",
            "requested topic partitions and actual Spark launch master agree",
            "training source, artifacts and implementation source remain unchanged",
        ],
        "timing_scope": {
            "throughput": "raw sink rows / fresh worker wall time, including Python/JVM startup and shutdown",
            "query_elapsed": "stream() receipt elapsed; excludes initial SparkSession construction",
            "inference": "stored model.predict latency, including feature DataFrame conversion",
            "end_to_end": "Kafka record timestamp to prediction; includes intentionally preloaded backlog and JVM startup",
        },
        "resource_scope": {
            "sample_interval_seconds": 0.1,
            "window": "publication start through worker completion; services already started",
            "rss": "sampled resident bytes; simultaneous sums double-count shared pages and are not total host memory",
            "cpu": "sampled per-process user+system CPU deltas; short-lived processes and final sampling tails may be missed",
            "runner_cpu": "exact process_time delta for benchmark Python, including producer and sampler threads",
            "children_rusage": "OS CPU delta for reaped children, separately reported; do not add to sampled child CPU",
        },
        "interpretation": "four sequential single observations; cold JVM each time, shared warm services/OS cache; no statistical speedup, Big Data, multi-node, cost, Databricks, or real-plant claim",
        "performance_acceptance_threshold": None,
    }


def tree_hashes(directory):
    require(directory.is_dir(), f"Missing stopped state directory: {directory}")
    entries = sorted(directory.rglob("*"))
    require(not any(path.is_symlink() for path in entries), "State snapshots must not contain symlinks")
    files = [path for path in entries if path.is_file()]
    require(len(files) <= 5000 and sum(path.stat().st_size for path in files) <= 512 * 1024**2,
            "Training snapshot exceeds the bounded copy profile")
    return {str(path.relative_to(directory)): file_hash(path) for path in files}


def copy_training(prior, work, expected_source):
    before = {}
    for relative in ("services/mlflow", "training"):
        before[relative] = tree_hashes(prior / relative)
        shutil.copytree(prior / relative, work / relative)
        require(tree_hashes(prior / relative) == before[relative], "Source state changed while copying")
        require(tree_hashes(work / relative) == before[relative], "Copied training state differs")
    copied = json.loads((work / "training/training-report.json").read_text())
    require(copied["training_source"] == expected_source, "Copied training source is incompatible")
    return before, copied


def free_ports():
    for port in (5000, 19092, 19093):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(f"Port {port} is occupied; wait for qualification to stop") from error


class ResourceSampler:
    """Observe this runner's children only; never inspect unrelated workloads."""

    def __init__(self, services, worker_name, path):
        self.services, self.worker_name, self.path = services, worker_name, path
        self.started = time.time()
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.processes, self.peaks, self.masters = {}, {}, set()
        self.samples, self.errors = 0, 0

    def start(self):
        self.thread.start()

    def sample(self):
        roles = {os.getpid(): "runner_python"}
        for name, child in list(self.services.children.items()):
            role = "worker_python" if name == self.worker_name else name + "_tree"
            roles[child.pid] = role
            try:
                for process in psutil.Process(child.pid).children(recursive=True):
                    roles[process.pid] = "worker_descendants" if name == self.worker_name else role
            except psutil.Error:
                continue
        totals, rows = {}, []
        for pid, role in roles.items():
            try:
                process = psutil.Process(pid)
                with process.oneshot():
                    created = process.create_time()
                    cpu = process.cpu_times()
                    seconds = cpu.user + cpu.system
                    rss = process.memory_info().rss
                    name = process.name()
                    if role == "worker_descendants":
                        for argument in process.cmdline():
                            if argument.startswith("spark.master=local["):
                                self.masters.add(argument.split("=", 1)[1])
                key = f"{pid}:{created}"
                item = self.processes.setdefault(key, {
                    "pid": pid, "name": name, "role": role,
                    "initial_cpu_seconds": seconds if created < self.started else 0.0,
                    "last_cpu_seconds": seconds, "peak_rss_bytes": 0,
                })
                item["last_cpu_seconds"] = max(item["last_cpu_seconds"], seconds)
                item["peak_rss_bytes"] = max(item["peak_rss_bytes"], rss)
                totals[role] = totals.get(role, 0) + rss
                rows.append({"pid": pid, "role": role, "rss_bytes": rss, "cpu_seconds": seconds})
            except psutil.Error:
                continue
        totals["all_children"] = sum(value for role, value in totals.items() if role != "runner_python")
        for role, value in totals.items():
            self.peaks[role] = max(self.peaks.get(role, 0), value)
        self.samples += 1
        return {"at_epoch": time.time(), "processes": rows, "rss_by_scope_bytes": totals}

    def run(self):
        try:
            with self.path.open("w", encoding="utf-8") as output:
                while True:
                    output.write(canonical(self.sample()) + "\n")
                    output.flush()
                    if self.done.wait(.1):
                        break
        except Exception:
            self.errors += 1

    def stop(self):
        self.done.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            self.errors += 1
        cpu = {}
        for item in self.processes.values():
            role = item["role"]
            cpu[role] = cpu.get(role, 0.0) + max(0, item["last_cpu_seconds"] - item["initial_cpu_seconds"])
        return {
            "samples": self.samples, "sampler_errors": self.errors,
            "sampled_peak_rss_bytes_by_scope": self.peaks,
            "sampled_cpu_seconds_by_scope": cpu,
            "observed_spark_masters": sorted(self.masters),
            "processes": list(self.processes.values()), "raw_samples": str(self.path),
            "scope": protocol(PipelineConfig())["resource_scope"],
        }


def quantiles(values):
    values = sorted(float(value) for value in values if value is not None)
    require(all(math.isfinite(value) and value >= 0 for value in values), "Invalid latency sample")
    if not values:
        return {"samples": 0, "p50": None, "p95": None, "p99": None}
    result = {"samples": len(values)}
    for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99)):
        index = (len(values) - 1) * fraction
        low = int(index)
        result[name] = values[low] + (values[min(low + 1, len(values) - 1)] - values[low]) * (index - low)
    return result


def canonical_snapshot(store, directory):
    features = [json.loads(row[0]) for row in store.db.execute("SELECT body FROM features ORDER BY feature_id")]
    predictions = []
    for row in store.db.execute("SELECT body FROM predictions ORDER BY feature_id"):
        body = json.loads(row[0])
        predictions.append({key: body[key] for key in (
            "feature_id", "event_id", "feature_signature", "model_name", "model_version",
            "run_id", "mode", "threshold", "score",
        )})
    hashes = {}
    for name, rows in (("features", features), ("scores", predictions)):
        path = directory / f"canonical-{name}.jsonl"
        path.write_text("".join(canonical(row) + "\n" for row in rows), encoding="utf-8")
        hashes[name] = file_hash(path)
    return hashes, {row["feature_id"]: row for row in predictions}


def backlog(receipt, streaming):
    ends = {str(part): values["last"] + 1 for part, values in receipt["offsets"].items()}
    rows = [{"batch_id": None, "offsets_behind": receipt["acknowledged"], "basis": "new topic; all input prepublished"}]
    for progress in streaming.get("progress", []):
        for source in progress.get("sources", []):
            end = source.get("endOffset")
            end = json.loads(end) if isinstance(end, str) else end
            if isinstance(end, dict) and receipt["topic"] in end:
                consumed = end[receipt["topic"]]
                rows.append({"batch_id": progress.get("batchId"),
                             "offsets_behind": sum(max(0, offset - consumed.get(part, 0)) for part, offset in ends.items()),
                             "basis": "Spark source endOffset compared with producer acknowledgements"})
    return rows


def run_case(case, input_path, work, services, model, config, deadline, shadow):
    from industrial_mlops.streaming import produce

    directory = work / "variants" / case["name"]
    directory.mkdir(parents=True)
    db, checkpoint = directory / "monitor.db", directory / "checkpoints"
    worker_name = "benchmark-" + case["name"]
    report_path = directory / "spark-receipt.json"
    with Store(db, config) as store:
        activation = store.activate(model, shadow=shadow)
    result = {**case, "status": "running", "input_sha256": file_hash(input_path),
              "activation": activation, "topic": worker_name, "store": str(db),
              "checkpoint": str(checkpoint), "worker_log": str(work / "evidence" / (worker_name + ".log"))}
    write_json(directory / "metrics.json", result)
    sampler = ResourceSampler(services, worker_name, directory / "resource-samples.jsonl")
    cpu_start, child_start = time.process_time(), resource.getrusage(resource.RUSAGE_CHILDREN)
    window_started = time.perf_counter()
    sampler.start()
    try:
        receipt = produce(str(input_path), BOOTSTRAP, worker_name, partitions=case["partitions"], max_events=10000)
        result["publication"] = receipt
        write_json(directory / "producer-receipt.json", receipt)
        worker_budget = min(300, int(deadline - time.monotonic()))
        require(worker_budget >= 45, "Insufficient remaining total budget for another Spark worker")
        command = ["--bootstrap", BOOTSTRAP, "--topic", worker_name, "--db", str(db),
                   "--checkpoint", str(checkpoint), "--tracking-uri", TRACKING_URI,
                   "--query-id", worker_name, "--available-now", "--max-offsets", "200",
                   "--duration-seconds", str(worker_budget - 25), "--workers", str(case["workers"]),
                   "--report", str(report_path)]
        result["worker_command"] = command
        result["worker_wall_limit_seconds"] = worker_budget
        started = time.perf_counter()
        services.worker(worker_name, command, timeout=worker_budget)
        result["worker_wall_seconds"] = time.perf_counter() - started
    except BaseException as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        result["resources"] = sampler.stop()
        child_end = resource.getrusage(resource.RUSAGE_CHILDREN)
        result["resources"].update({
            "runner_cpu_seconds": time.process_time() - cpu_start,
            "reaped_children_cpu_seconds_delta": child_end.ru_utime + child_end.ru_stime - child_start.ru_utime - child_start.ru_stime,
            "runner_lifetime_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "measurement_wall_seconds": time.perf_counter() - window_started,
        })
        write_json(directory / "metrics.json", result)
    streaming = json.loads(report_path.read_text())
    result["spark"] = streaming
    result["backlog_progress"] = backlog(receipt, streaming)
    with Store(db, config, readonly=True) as store:
        verification, counts = store.verify(), store.counts()
        hashes, scores = canonical_snapshot(store, directory)
        result.update({"verification": verification, "counts": counts, "canonical_sha256": hashes})
        result["latency_ms"] = {
            "inference": quantiles(row[0] for row in store.db.execute("SELECT inference_ms FROM predictions")),
            "broker_to_prediction": quantiles(row[0] for row in store.db.execute("SELECT arrival_to_prediction_ms FROM predictions")),
        }
        result["batches"] = [dict(row) for row in store.db.execute(
            "SELECT batch_id,rows_received,committed_at FROM batches ORDER BY batch_id")]
    lag = streaming.get("source_lag", {})
    result["observed_partition_count"] = len(lag.get("partitions", [])) if lag.get("status") == "observed" else None
    result["raw_records_per_worker_wall_second"] = counts["raw"] / result["worker_wall_seconds"]
    result["query_elapsed_seconds_reported"] = streaming.get("elapsed_seconds")
    result["checks"] = {
        "source_ack_matches": counts["raw"] == receipt["acknowledged"],
        "invariants": verification["valid"],
        "drained": streaming.get("drained") is True,
        "source_lag_zero": lag.get("status") == "observed" and lag.get("offsets_behind") == 0,
        "partition_count": result["observed_partition_count"] == case["partitions"],
        "spark_version": streaming.get("spark_version") == importlib.metadata.version("pyspark"),
        "spark_workers": streaming.get("workers") == case["workers"] and result["resources"]["observed_spark_masters"] == [f"local[{case['workers']}]"],
        "all_features_scored": counts["features"] > 0 and counts["pending_predictions"] == 0,
        "candidate_identity": bool(scores) and all(row["model_version"] == str(model.model_version) and row["run_id"] == model.run_id for row in scores.values()),
        "latencies_present": all(value["samples"] == counts["predictions"] for value in result["latency_ms"].values()),
        "bounded_batches": bool(result["batches"]) and max(row["rows_received"] for row in result["batches"]) <= 200,
        "backlog_exercised": case["hours"] != 8 or receipt["acknowledged"] > 200 and len(result["batches"]) > 1,
        "resources_observed": result["resources"]["samples"] > 1 and result["resources"]["sampler_errors"] == 0,
    }
    result["status"] = "passed" if all(result["checks"].values()) else "failed"
    write_json(directory / "metrics.json", result)
    return result, scores


def cleanup(services):
    """Stop only owned process groups; collectively bound graceful shutdown."""
    children = list(services.children.items())
    for _, child in children:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    until = time.monotonic() + 5
    while any(child.poll() is None for _, child in children) and time.monotonic() < until:
        time.sleep(.1)
    for _, child in children:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
    services.children.clear()
    services.close()
    return {name: {"pid": child.pid, "returncode": child.poll()} for name, child in children}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", help="new isolated directory; an existing path is rejected")
    parser.add_argument("--tools", default=".tools")
    parser.add_argument("--reuse-training-from", default="state/native-qualification-01")
    parser.add_argument("--shadow", action="store_true", help="explicitly permit candidate shadow activation")
    parser.add_argument("--print-protocol", action="store_true", help="print the protocol without starting services or writing state")
    args = parser.parse_args()
    config = PipelineConfig()
    plan = protocol(config, args.shadow)
    if args.print_protocol:
        print(json.dumps(plan, indent=2))
        return 0
    require(args.work_dir, "--work-dir is required for execution")
    require(sys.platform == "linux", "This native reference requires Linux and its JDK/Kafka distributions")
    os.chdir(ROOT)
    work, prior, tools = Path(args.work_dir).resolve(), Path(args.reuse_training_from).resolve(), Path(args.tools).resolve()
    require(not work.exists(), "Benchmark requires a new work directory; preserve previous evidence")
    require(prior not in work.parents, "Benchmark output must be outside the source training state")
    require((tools / "jdk17/bin/java").is_file(), "Prepare the official native tools first")
    free_ports()
    (work / "evidence").mkdir(parents=True)
    started, deadline = time.monotonic(), time.monotonic() + 580
    source = source_manifest(ROOT)
    runner_hash, helper_hash = file_hash(Path(__file__)), file_hash(ROOT / "scripts/qualify_native.py")
    report = {"schema_version": 1, "status": "running", "started_at": time.time(), "protocol": plan,
              "source": source, "runner_sha256": runner_hash, "services_helper_sha256": helper_hash,
              "workloads": [], "checks": {}, "runtime": {
                  "python": platform.python_version(), "platform": platform.platform(),
                  "packages": {name: importlib.metadata.version(name) for name in (
                      "pyspark", "mlflow", "scikit-learn", "numpy", "pandas", "kafka-python", "psutil")},
              }}
    write_json(work / "evidence/protocol.json", {"protocol": plan, "source": source,
                                                "runner_sha256": runner_hash, "services_helper_sha256": helper_hash})
    write_json(work / "evidence/benchmark.json", report)
    services = Services(work, tools)

    def expired(_signal, _frame):
        raise TimeoutError("The 580-second work budget expired; cleanup has a reserved 20-second budget")

    previous_handler = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, max(.1, deadline - time.monotonic()))
    exit_code = 1
    try:
        from industrial_mlops.modeling import training_source_manifest
        from industrial_mlops.registry import RegistryClient
        from industrial_mlops.simulator import generate_dataset

        expected_training = training_source_manifest()
        original_report = json.loads((prior / "training/training-report.json").read_text())
        require(original_report["training_source"] == expected_training, "Training source changed; reuse is rejected")
        copied_hashes, training = copy_training(prior, work, expected_training)
        report["training_reuse"] = {"source": str(prior), "files_before_startup": copied_hashes,
                                    "training_source": expected_training, "new_training_run": False,
                                    "quiescence": "operator-supplied stopped snapshot; before/after copy hashes equal"}
        report["runtime"]["java"] = subprocess.check_output(
            [str(tools / "jdk17/bin/java"), "-version"], stderr=subprocess.STDOUT, text=True, timeout=10)
        verified = subprocess.run([sys.executable, str(ROOT / "scripts/fetch_spark_jars.py"),
                                   "--offline", "--output", str(tools / "spark-jars")],
                                  capture_output=True, text=True, timeout=20)
        (work / "evidence/jar-verification.log").write_text(verified.stdout + verified.stderr)
        verified.check_returncode()
        shutil.copy2(tools / "spark-jars/verification.json", work / "evidence/jar-verification.json")
        inputs = {}
        for hours in (2, 8):
            dataset = generate_dataset(seed=42, machines=4, sites=2, hours=hours,
                                       start=plan["start"], anomalies=True,
                                       horizon_seconds=config.horizon_seconds, lateness_seconds=config.lateness_seconds)
            path = work / f"input-{hours:02d}h.jsonl"
            path.write_text("".join((event if isinstance(event, str) else canonical(event)) + "\n"
                                    for event in dataset["events"]), encoding="utf-8")
            write_json(path.with_suffix(".manifest.json"), dataset["manifest"])
            inputs[hours] = path
        report["inputs"] = {str(hours): {"path": str(path), "sha256": file_hash(path)} for hours, path in inputs.items()}
        emit("starting_benchmark_services")
        services.kafka(initialize=True)
        report["kafka_readiness"] = json.loads((work / "evidence/kafka-ready.json").read_text())
        services.mlflow()
        candidate = training["models"]["candidate"]
        model = RegistryClient(TRACKING_URI).load(str(candidate["model_version"]))
        require(model.model_digest == candidate["model_digest"] and model.run_id == candidate["run_id"],
                "Reused registry identity differs from the training receipt")
        require(model.qualified or args.shadow, "Candidate is unqualified; explicit --shadow is required")
        report["selected_model"] = {"version": str(model.model_version), "run_id": model.run_id,
                                    "digest": model.model_digest, "qualified": model.qualified,
                                    "mode": "shadow" if args.shadow else "approved"}
        reference, reference_scores = None, None
        comparisons = []
        for case in CASES:
            emit("running_benchmark_variant", **case)
            result, scores = run_case(case, inputs[case["hours"]], work, services, model, config, deadline, args.shadow)
            report["workloads"].append(result)
            if case["hours"] == 8:
                if reference is None:
                    reference, reference_scores = result, scores
                else:
                    keys_equal = scores.keys() == reference_scores.keys()
                    differences = [abs(scores[key]["score"] - reference_scores[key]["score"]) for key in scores] if keys_equal else []
                    identity_equal = keys_equal and all(
                        {k: v for k, v in scores[key].items() if k != "score"} ==
                        {k: v for k, v in reference_scores[key].items() if k != "score"} for key in scores)
                    comparisons.append({"reference": reference["name"], "variant": case["name"],
                                        "same_input": result["input_sha256"] == reference["input_sha256"],
                                        "canonical_features_equal": result["canonical_sha256"]["features"] == reference["canonical_sha256"]["features"],
                                        "canonical_scores_exact": result["canonical_sha256"]["scores"] == reference["canonical_sha256"]["scores"],
                                        "score_identity_equal": identity_equal,
                                        "maximum_absolute_score_difference": max(differences) if differences else None,
                                        "scores_within_tolerance": bool(differences) and max(differences) <= 1e-12})
            report["comparisons"] = comparisons
            write_json(work / "evidence/benchmark.json", report)
            require(result["status"] == "passed", f"Variant {case['name']} failed its declared checks")
        report["checks"] = {
            "all_variants_passed": len(report["workloads"]) == 4 and all(row["status"] == "passed" for row in report["workloads"]),
            "fixed_input_parity": len(comparisons) == 2 and all(all(row[key] for key in (
                "same_input", "canonical_features_equal", "score_identity_equal", "scores_within_tolerance")) for row in comparisons),
            "source_unchanged": source_manifest(ROOT)["identity"] == source["identity"],
            "runner_unchanged": file_hash(Path(__file__)) == runner_hash and file_hash(ROOT / "scripts/qualify_native.py") == helper_hash,
            "training_source_unchanged": training_source_manifest() == expected_training,
            "original_snapshot_unchanged": all(tree_hashes(prior / relative) == hashes for relative, hashes in copied_hashes.items()),
            "copied_training_unchanged": tree_hashes(work / "training") == copied_hashes["training"],
            "copied_model_artifacts_unchanged": tree_hashes(work / "services/mlflow/artifacts") == tree_hashes(prior / "services/mlflow/artifacts"),
        }
        require(all(report["checks"].values()), "Benchmark acceptance or source identity check failed")
        report["status"], exit_code = "passed", 0
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        for path in (work / "variants").glob("*/metrics.json"):
            unfinished = json.loads(path.read_text())
            if unfinished.get("status") == "running":
                unfinished.update({"status": "failed", "error": report["error"]})
                write_json(path, unfinished)
        emit("benchmark_failed", **report["error"])
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        report["services_stopped"] = cleanup(services)
        report["checks"]["owned_services_stopped"] = all(
            item["returncode"] is not None for item in report["services_stopped"].values())
        report["elapsed_seconds"] = time.monotonic() - started
        report["within_total_wall_budget"] = report["elapsed_seconds"] <= 600
        if not report["within_total_wall_budget"] or not report["checks"]["owned_services_stopped"]:
            report["status"], exit_code = "failed", 1
        report["finished_at"] = time.time()
        write_json(work / "evidence/benchmark.json", report)
        emit("benchmark_finished", status=report["status"], evidence=str(work / "evidence/benchmark.json"))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
