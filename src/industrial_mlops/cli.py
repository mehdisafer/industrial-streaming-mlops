"""Explicit operator commands; no command automatically promotes or exposes a service."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from industrial_mlops.config import PipelineConfig
from industrial_mlops.operations import source_manifest, write_json
from industrial_mlops.store import Store


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="industrial-mlops")
    p.add_argument("--config", default=os.environ.get("PIPELINE_CONFIG"))
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("generate", "train", "produce", "stream", "dashboard", "verify", "promote",
                 "activate", "rollback", "score", "monitor", "backup", "restore", "benchmark"):
        child = sub.add_parser(name)
        child.add_argument("--report", help="write machine-readable result to this path")
        child.add_argument("--db", default=os.environ.get("PIPELINE_DB", "state/pipeline/monitor.db"))
        child.add_argument("--tracking-uri", default=os.environ.get(
            "MLFLOW_TRACKING_URI", "http://127.0.0.1:5000"))
        if name in ("generate", "train"):
            child.add_argument("--output", required=True)
            child.add_argument("--machines", type=int, default=6)
            child.add_argument("--hours", type=int, default=72)
        if name == "generate":
            child.add_argument("--seed", type=int, default=42)
            child.add_argument("--sites", type=int, default=2)
            child.add_argument("--start", default="2026-05-01T00:00:00Z")
            child.add_argument("--anomalies", action="store_true")
            child.add_argument("--drift", type=float, default=0)
        if name in ("produce", "stream"):
            child.add_argument("--bootstrap", default=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"))
            child.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "telemetry"))
        if name == "produce":
            child.add_argument("--input", required=True)
            child.add_argument("--partitions", type=int, default=3)
            child.add_argument("--max-events", type=int, default=100000)
            child.add_argument("--rate", type=float, default=0)
        if name == "stream":
            child.add_argument("--checkpoint", default=os.environ.get("CHECKPOINT_DIR", "state/checkpoints"))
            child.add_argument("--query-id", default="industrial-v1")
            child.add_argument("--available-now", action="store_true")
            child.add_argument("--duration-seconds", type=int, default=300)
            child.add_argument("--max-offsets", type=int, default=1000)
            child.add_argument("--workers", type=int, default=2)
            child.add_argument("--failpoint", choices=["before_commit", "after_commit"])
            child.add_argument("--fail-batch", type=int, default=0)
            child.add_argument("--fail-marker")
            child.add_argument("--ready-file", help="write query identity after a worker query starts")
        if name in ("promote", "activate"):
            child.add_argument("--version", required=True)
        if name == "activate":
            child.add_argument("--shadow", action="store_true")
        if name == "verify":
            child.add_argument("--output")
        if name == "dashboard":
            child.add_argument("--host", default="127.0.0.1")
            child.add_argument("--port", type=int, default=8080)
        if name == "monitor":
            child.add_argument("--as-of", type=float, default=None)
        if name == "backup":
            child.add_argument("--state-dir", default="state")
            child.add_argument("--output", required=True)
            child.add_argument("--quiesced", action="store_true")
        if name == "restore":
            child.add_argument("--input", required=True)
            child.add_argument("--target", required=True)
        if name == "benchmark":
            child.add_argument("--output", required=True)
            child.add_argument("--machines", type=int, default=4)
            child.add_argument("--hours", type=int, nargs="+", default=[2, 8, 24])
    return p


def execute(args, config: PipelineConfig):
    if args.command == "generate":
        from industrial_mlops.simulator import generate_dataset
        data = generate_dataset(
            seed=args.seed, machines=args.machines, sites=args.sites, hours=args.hours,
            start=args.start, anomalies=args.anomalies, drift=args.drift,
            horizon_seconds=config.horizon_seconds, lateness_seconds=config.lateness_seconds,
        )
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as handle:
            for event in data["events"]:
                handle.write((event if isinstance(event, str) else json.dumps(event, sort_keys=True)) + "\n")
        write_json(output.with_suffix(".manifest.json"), data["manifest"])
        return data["manifest"]
    if args.command == "train":
        from industrial_mlops.modeling import train_and_register
        manifest = source_manifest()
        write_json(Path(args.output) / "source-manifest.json", manifest)
        return train_and_register(
            args.tracking_uri, args.output, config, manifest["identity"],
            machines=args.machines, hours=args.hours,
        )
    if args.command == "produce":
        from industrial_mlops.streaming import produce
        return produce(args.input, args.bootstrap, args.topic, partitions=args.partitions,
                       max_events=args.max_events, rate=args.rate)
    if args.command == "stream":
        from industrial_mlops.streaming import stream
        def ready(query):
            if args.ready_file:
                write_json(args.ready_file, {"query_id": str(query.id), "run_id": str(query.runId)})
        return stream(
            bootstrap=args.bootstrap, topic=args.topic, db_path=args.db, checkpoint=args.checkpoint,
            tracking_uri=args.tracking_uri, config=config, query_id=args.query_id,
            available_now=args.available_now, duration_seconds=args.duration_seconds,
            max_offsets=args.max_offsets, workers=args.workers, failpoint=args.failpoint,
            fail_batch=args.fail_batch, fail_marker=args.fail_marker,
            on_started=ready,
        )
    if args.command == "dashboard":
        from industrial_mlops.dashboard import serve
        serve(args.db, config, args.host, args.port)
        return None
    if args.command == "verify":
        with Store(args.db, config, readonly=True) as store:
            report = store.verify()
        if args.output:
            write_json(args.output, report)
        if not report["valid"]:
            raise RuntimeError("persisted invariants failed; inspect the verification report")
        return report
    if args.command in ("promote", "activate", "rollback", "score", "monitor"):
        from industrial_mlops.registry import RegistryClient
        registry = RegistryClient(args.tracking_uri)
        if args.command == "promote":
            return registry.promote(args.version)
        with Store(args.db, config) as store:
            if args.command == "activate":
                return store.activate(registry.load(version=args.version), shadow=args.shadow)
            if args.command == "rollback":
                previous = store.previous_deployment()
                model = registry.load(version=previous["model_version"])
                return store.activate(model, shadow=previous["mode"] == "shadow", action="rollback")
            active = store.deployment()
            if not active:
                raise ValueError("activate a numeric model version before this operation")
            model = registry.load(version=active["model_version"])
            if args.command == "score":
                return {"predictions_written": store.score_pending(model), "counts": store.counts()}
            from industrial_mlops.observability import monitor_store
            result = monitor_store(store, model, as_of=args.as_of)
            store.set_runtime("monitoring", result)
            return result
    if args.command == "backup":
        from industrial_mlops.operations import backup_state
        return backup_state(args.state_dir, args.output, quiesced=args.quiesced)
    if args.command == "restore":
        from industrial_mlops.operations import restore_state
        return restore_state(args.input, args.target)
    if args.command == "benchmark":
        from industrial_mlops.benchmark import benchmark_local
        return benchmark_local(args.output, config, args.machines, args.hours)
    raise ValueError("unsupported command")


def main():
    args = parser().parse_args()
    config = PipelineConfig.load(args.config) if args.config else PipelineConfig()
    result = execute(args, config)
    if result is not None:
        if args.report:
            write_json(args.report, result)
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
