"""Bounded local sink workloads. Kafka/Spark measurements live in integration evidence."""

from __future__ import annotations

import json
import resource
import tempfile
import time
from pathlib import Path

from industrial_mlops.operations import source_manifest, write_json
from industrial_mlops.simulator import generate_dataset
from industrial_mlops.store import Store


def benchmark_local(output, config, machines=4, hours=(2, 8, 24)):
    if not 1 <= machines <= 32 or not hours or len(hours) > 5 or max(hours) > 72 or min(hours) < 1:
        raise ValueError("local benchmark exceeds the bounded reference workload")
    result = {
        "scope": "single-process local SQLite/feature sink; excludes Kafka, Spark and model inference",
        "protocol": {"machines": machines, "hours": list(hours), "batch_rows": 500,
                     "acceptance": "all persisted invariants true; no performance gate or scale claim"},
        "source": source_manifest(), "workloads": [],
    }
    for duration in hours:
        data = generate_dataset(seed=42, machines=machines, hours=duration)
        records = [{"topic": "benchmark", "partition": 0, "offset": i,
                    "value": event if isinstance(event, str) else json.dumps(event, sort_keys=True)}
                   for i, event in enumerate(data["events"])]
        with tempfile.TemporaryDirectory(prefix="industrial-bench-") as temp:
            started_cpu, started = time.process_time(), time.perf_counter()
            with Store(Path(temp) / "sink.db", config) as store:
                for batch_id, offset in enumerate(range(0, len(records), 500)):
                    store.process_batch(records[offset:offset + 500], "benchmark", batch_id)
                elapsed = time.perf_counter() - started
                verification = store.verify()
                if not verification["valid"]:
                    raise RuntimeError("benchmark invariant failed")
                result["workloads"].append({
                    "hours": duration, "raw_records": len(records), "elapsed_seconds": elapsed,
                    "cpu_seconds": time.process_time() - started_cpu,
                    "records_per_second": len(records) / elapsed,
                    "process_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    "verification": verification,
                })
    write_json(output, result)
    return result
