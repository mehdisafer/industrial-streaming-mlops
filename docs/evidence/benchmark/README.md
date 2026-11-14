# Bounded streaming benchmark evidence

[Benchmark03](benchmark.json) completed all four predeclared variants and all 10 overall checks in **84.613667 seconds**. Each 24-hour variant consumed **2,317 raw records**, produced **1,141 predictions** and persisted **4 alerts**. Of 22 above-threshold predictions, **18 were suppressed by the configured cooldown**. The two comparisons with the one-partition reference have identical canonical features, scores, alert business content and cooldown outcomes; the maximum observed score difference is **0.0**.

This is one sequential observation per configuration on a single host. The results establish the stated correctness checks for this synthetic fixture. They do not establish a reproducible speedup, throughput capacity, a production memory requirement, distributed scalability, cloud cost, Databricks qualification or real-plant accuracy. The four emitted alerts are evidence of nonzero alert processing, not a claim of four independently validated real-world failures.

## What ran

The [predeclared protocol](protocol.json) uses four machines, two sites, seed 42, synthetic coverage starting at `2026-06-01T00:00:00Z`, and explicit duplicate, malformed and late-data cases. “2h” and “24h” describe simulated data coverage, not elapsed execution time. All three 24-hour cases reuse exactly the same [input bytes](inputs/input-24h.jsonl.gz); the [input manifest](inputs/input-24h.manifest.json) records generation provenance.

Each case has its own Kafka topic, SQLite sink and Spark checkpoint, and starts a fresh CLI/Python/JVM worker. Kafka and MLflow services are shared between sequential cases. Candidate model **version 2** was loaded from copied, unchanged training state and activated in `approved` mode. The benchmark performed no training or promotion and provisioned no cloud resources. The model run ID is `fc11d0d4b21e4ba2900a4162b93df8e1` and the serialized model digest is `034e2dfb9204865f9684f1da73fe201264071e4a225af01de41e68ce7ab3deac`.

`maxOffsetsPerTrigger=200` exposed a preloaded backlog: the 24-hour cases processed 12 committed batches each; the 2-hour case processed 2. All producer acknowledgements balanced with stored raw records, all final observed source lags were zero, and no prediction or sensor remained pending. The complete records remain in each producer receipt, Spark receipt and metrics file. The limit bounds micro-batches; it is not a demonstrated continuous arrival-rate capacity.

The worker limit was at most 300 seconds each. A 580-second work deadline reserved 20 seconds for cleanup within a 600-second overall budget. The [original console](console.log), [Kafka log](logs/kafka.log), [MLflow log](logs/mlflow.log) and four `logs/benchmark-*.log` files preserve the observed execution output. Kafka and MLflow were stopped at completion according to the original receipt.

## Observed workload and throughput

| Variant | Simulated hours | Partitions | Local threads | Raw records | Predictions | Alerts | Worker wall s | Raw records/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [h02-p1-w1](variants/h02-p1-w1/metrics.json) | 2 | 1 | 1 | 205 | 85 | 0 | 13.294 | 15.42 |
| [h24-p1-w1](variants/h24-p1-w1/metrics.json) | 24 | 1 | 1 | 2,317 | 1,141 | 4 | 16.182 | 143.18 |
| [h24-p3-w1](variants/h24-p3-w1/metrics.json) | 24 | 3 | 1 | 2,317 | 1,141 | 4 | 18.956 | 122.23 |
| [h24-p3-w2](variants/h24-p3-w2/metrics.json) | 24 | 3 | 2 | 2,317 | 1,141 | 4 | 18.645 | 124.27 |

Worker wall time includes fresh Python/JVM startup and shutdown. Throughput is stored raw records divided by that wall time. It counts sensor, label, control, duplicate and rejected messages. Services start before the variant timing window; `query_elapsed_seconds_reported` in the metrics uses the separate `stream()` scope and excludes initial SparkSession construction. Four sequential measurements with warm shared services and OS caches do not support significance or scaling claims.

## Observed latency

| Variant | Inference p50 ms | Inference p95 ms | Inference p99 ms | Broker-to-prediction p50 s | Broker-to-prediction p95 s | Broker-to-prediction p99 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| h02-p1-w1 | 0.666 | 1.025 | 2.172 | 11.726 | 11.754 | 11.756 |
| h24-p1-w1 | 0.672 | 0.946 | 1.461 | 12.023 | 14.904 | 15.261 |
| h24-p3-w1 | 0.677 | 0.988 | 1.640 | 12.963 | 17.450 | 17.921 |
| h24-p3-w2 | 0.680 | 1.110 | 1.705 | 13.191 | 17.142 | 17.485 |

Inference timing includes feature DataFrame conversion in `model.predict`. Broker-to-prediction latency starts at the Kafka record timestamp and includes the intentionally preloaded backlog and worker startup. The fixtures' historical business timestamps are not used as wall-clock latency origins. There are 85 prediction samples for the 2-hour case and 1,141 for each 24-hour case. The displayed values are rounded summaries; original JSON retains full precision. No performance acceptance threshold or confidence interval was declared.

## Resource scope and observations

The sampler targets a 0.1-second interval from publication start through worker completion. It observes the benchmark Python process and only its owned subprocess trees. “Worker descendants” includes the Spark JVM; the Python worker is reported separately. Per-role RSS is a simultaneous sum within that role, and “all children” is the largest simultaneous sum across owned child roles, excluding the benchmark runner.

### Sampled peak RSS, MiB

| Variant | Runner Python | Worker Python | Worker descendants | Kafka tree | MLflow tree | All children |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| h02-p1-w1 | 265.74 | 268.23 | 434.71 | 354.44 | 2385.16 | 3441.10 |
| h24-p1-w1 | 268.61 | 285.62 | 593.15 | 360.09 | 2385.23 | 3623.46 |
| h24-p3-w1 | 278.29 | 285.94 | 750.32 | 363.88 | 2385.31 | 3766.18 |
| h24-p3-w2 | 281.35 | 285.59 | 652.01 | 372.78 | 2374.85 | 3682.21 |

MiB means 1,048,576 bytes. RSS sums count shared mappings in multiple processes; they are not unique physical memory or total host RAM usage. Peaks of separate columns can occur at different instants and must not be added. Sampling can miss short-lived peaks. `runner_lifetime_peak_rss_bytes` is also preserved in the metrics, but its cumulative process-wide scope differs from the per-variant sampled values above.

### CPU seconds

| Variant | Runner process CPU | Worker Python sampled | Worker descendants sampled | Kafka sampled | MLflow sampled | Reaped children OS CPU delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| h02-p1-w1 | 0.518 | 3.710 | 17.990 | 1.430 | 0.660 | 3.847 |
| h24-p1-w1 | 0.940 | 5.220 | 25.850 | 1.560 | 0.540 | 5.282 |
| h24-p3-w1 | 0.859 | 5.370 | 25.200 | 1.000 | 0.380 | 5.430 |
| h24-p3-w2 | 0.877 | 5.260 | 25.240 | 0.980 | 0.470 | 5.431 |

Runner process CPU is the measured `process_time` delta, including producer and sampler threads. Child columns are sampled user+system CPU deltas; short-lived processes and final tails can be missed. Reaped-child CPU is an independent operating-system statistic with a different scope. It overlaps the sampled observations and must not be added to them. All four samplers reported zero sampler errors. Compressed `resource-samples.jsonl.gz` files preserve the raw observations.

## Alert and cooldown equivalence

Each 24-hour case recorded 22 above-threshold predictions, the same 4 emitted alerts and the same 18 non-emitted predictions explained by the 1,800-second cooldown. Local checks verified sensor-window evidence, score/model consistency, unique alert features and an independently replayed business-time cooldown schedule. This closes the zero-alert coverage limitation of benchmark02.

Canonical alert records retain identities, business timestamps, observations, evidence, score, threshold, model/run identity, mode and reason. They exclude execution timing fields `predicted_at_epoch`, `inference_ms` and `arrival_to_prediction_ms`, plus `created_at` if present; the observed schema had no `created_at`. `event_time` and `available_at` remain in the comparison. Original database content was not rewritten; this package includes the canonical textual snapshots instead of database binaries.

The following **uncompressed** SHA-256 values are equal in all three 24-hour cases. The [packaging manifest](manifest.json) separately hashes the deterministic gzip files.

| Canonical content | Equal SHA-256 across the three 24-hour variants |
| --- | --- |
| features | `4caa4dab8572a56cb8fd06f5bdca1a91a728cb8d99557934bf2c9258755cefac` |
| scores | `51845092434cce336cd621fd5d8da60c723960d991fa84c749bd77be8e753b4b` |
| alerts | `6cf04022fa55afe17d566f7b5ca4966a94a08dbc5f7340e943cec1bea7d8f7f7` |
| cooldown | `ad0df3abcae829bd176d434359d25b839c72d1e9800bb70ccda3d908db939dc1` |

The 2-hour case contains no alerts and no above-threshold predictions. Nonzero-alert and suppression requirements apply to the three 24-hour variants; a true conditional check in the small case must not be read as evidence that it emitted an alert.

## Source and runtime identity

The run receipt records source commit `349ec4c892c15b0deeeb34c81a2cc8baeecbf656` and runtime source identity:

```text
working-tree-sha256:8756108fa9d3f07592b2a40e51f9da1a07baa2314bda77e413af08b23d930866
```

Its runner SHA-256 is `5ac9fb4de43d42f2b47c3e050b01e922eddea9526e73bc6c9b2387865c50d795`; the Services helper SHA-256 is `8b395533b92e8cc5886803227fa756de6e757c972bedb73f733b3e11b050e40d`. Runtime source identity, all six package versions shared by the native08 receipt, Python and platform match native08; the benchmark additionally records psutil. [Runtime comparison](provenance/runtime-comparison.json) records those exact checks and both original receipt hashes. The commit identifier is preserved from the execution receipt; this evidence package does not independently claim a local Git-object verification.

| Recorded component | Version |
| --- | --- |
| Python | 3.11.18 |
| Java | Temurin 17.0.21+9 |
| kafka-python | 2.3.2 |
| mlflow | 3.17.2 |
| numpy | 2.3.5 |
| pandas | 2.2.3 |
| psutil | 7.2.2 |
| pyspark | 3.5.9 |
| scikit-learn | 1.8.0 |

Platform: `Linux-6.18.57-x86_64-with-glibc2.41`. Recorded start: `2026-11-13T23:55:20.652462+00:00`; finish: `2026-11-13T23:56:45.261891+00:00`. [JAR verification](provenance/jar-verification.json) retains the already verified connector/dependency identities; no JARs or model binaries are included here.

## Earlier evidence retained with its original scope

- [Prior 2h/8h streaming run](prior-8h/README.md): original benchmark02 report, protocol and console retained. It passed its original feature/score checks but all variants emitted zero alerts; it cannot establish positive alert or cooldown equivalence.
- [Startup failure](startup-failure/receipt.json): original benchmark01 console and failure receipt retained. Missing psutil in the clean locked Python environment prevented work-directory creation and service startup. The original failure is not relabelled as a successful run.
- [Local sink benchmark](local-sink/README.md): original JSON and console from an earlier, distinct source identity. Its SQLite/feature-only timings exclude Kafka, Spark, registry access and model inference and cannot be compared with streaming throughput as equivalent workloads.

## Packaging and inspection

Original JSON and logs are copied byte for byte. Inputs, canonical JSONL and raw resource samples use gzip level 9 with `mtime=0` and no embedded original filename. Every gzip payload was decompressed and compared byte for byte with its original; a second compression produced identical bytes. Original hashes and compressed hashes are recorded separately in [manifest.json](manifest.json). Original local paths and timestamps remain inside receipts; the manifest maps them to package paths.

[Content scan](content-scan.json) applies the repository's credential-pattern rules to every original text file and every packaged text/decompressed gzip payload. It records locations and rule names only and never prints matched values. No pattern matches were found. This is a bounded pattern inspection, not entropy analysis or hosted credential validation. Synthetic input payloads intentionally include malformed records used by the contract tests.

No SQLite database, checkpoint directory, Kafka storage segment, serialized model, JAR or executable binary is packaged. The manifest itself is excluded from its own hash list to avoid a circular hash; the enclosing evidence index can bind its final bytes.
