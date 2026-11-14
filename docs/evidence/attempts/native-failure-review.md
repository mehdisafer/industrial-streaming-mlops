# Native qualification failure review

This review covers native attempts **04** and **05**. Both aggregate runs remain failed. Their original logs and persistent state were retained. The reviewer did not change source code, start or stop services, delete evidence, or replay workloads. The accompanying JSON records original file paths, SHA-256 digests, timestamps, runtime versions and read-only database observations.

## Attempt 04: the gateway disappeared after committed writes

The original console log records a successful rollback-stage `batch_committed` entry for batch 6: 4,380 raw Kafka records and 2,092 predictions, including 303 newly processed coordinates and 127 new predictions. A read-only SQLite inspection confirms **4,380 raw records, 2,092 features, 2,092 predictions, 12 alerts and 7 batch-ledger entries**. The deployment history is baseline version 1, candidate version 2, then rollback to version 1. Predictions are present for both versions: 1,965 for version 1 and 127 for version 2.

The subsequent `query.awaitTermination` call lost the Java gateway: Py4J first reported an empty Java-side answer, then connection refusal. Cleanup called `query.isActive` through the lost gateway and raised another connection-refused error. The JVM exit signal was not captured. No fatal JVM report or explicit Java out-of-memory message was found in the reviewed repository evidence.

The recorded checks already establish initial ingestion, pending retention during model unavailability, model recovery, batch/stream feature and score parity, both transactional fault boundaries, and active-query broker outage recovery. They do **not** establish completion of the later dashboard and full restoration stages. The committed effects must be preserved even though aggregate qualification failed.

## Attempt 05: a long runtime stall before Spark started

Kafka reports that the broker was unfenced at **00:20:21.084** and that the server started at **00:20:21.092**, as logged on 2026-11-14. At **00:21:13.921**, a heartbeat with a configured 2,000 ms timeout was cancelled after **34,563 ms since send**. The MLflow/client model-load sequence also has an approximately **44-second gap**, from 00:20:29 to 00:21:13.

The attempt then failed in `create_topics` after a 60-second client timeout, before any Spark worker was launched. Kafka's cleanup log nevertheless records creation and loading of all three `qualification` partition logs at **00:22:33.717–00:22:33.751**. The timeout therefore cannot be interpreted as “nothing was created.” A successful post-timeout Kafka metadata readback was not captured in this attempt.

These timings support investigation of a broader scheduling, runtime-pause or resource stall. A simple early TCP-readiness race does not, on its own, explain the long gaps after the broker had already reported a running state. Repeated Spark contexts also cannot explain the failure *within attempt 05*, because that attempt had not started Spark.

## What remains unproven

The evidence does not identify an out-of-memory kill, a JVM heap limit, an accumulation of orphan JVMs, a particular CPU quota, or a reproducible Kafka protocol defect. The review command sees its own process namespace; it cannot enumerate processes from earlier execution namespaces. Cgroup memory, CPU and pressure counters were unavailable. Post-failure memory and disk availability are recorded as observer context only; they do not establish failure-time peaks.

## Narrow next step

The current runner implements fresh Python/JVM workers with bounded waits and owned-process-group cleanup, and a Kafka metadata readiness check before publishing. Those are concrete lifecycle/readiness improvements. They are not a diagnosis or a successful qualification result. Attempt 06 was still in progress when this review was written.

Preserve the failed attempts and evaluate the next bounded run without changing the business gates. If a stall recurs, capture child PID/process-group identities, Java gateway exit status, bounded CPU/RSS/thread samples and JVM GC/safepoint logs **inside the execution namespace that owns the services**. Reconcile uncertain topic-creation outcomes through Kafka metadata before another mutation. Continue to preserve source coordinates, checkpoint identity and the transactional ledger when resuming existing data.

## Original evidence locations

- `state/native-qualification-04.log`
- `state/native-qualification-04/evidence/qualification.json`
- `state/native-qualification-04/evidence/kafka.log`
- `state/native-qualification-04/pipeline/monitor.db` — inspected read-only
- `state/native-qualification-05.log`
- `state/native-qualification-05/evidence/qualification.json`
- `state/native-qualification-05/evidence/kafka.log`
- `state/native-qualification-05/evidence/mlflow.log`

The JSON companion supplies file hashes and structured findings. Hashes identify the reviewed bytes; they are not independent certification.
