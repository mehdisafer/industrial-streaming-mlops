# Earlier local sink benchmark: separate scope

These [original results](benchmark.json) and [original console output](console.log) exercise `single-process local SQLite/feature sink; excludes Kafka, Spark and model inference`. They use four synthetic machines and batches of 500 records. No model is supplied: predictions remain zero and finalized features remain pending by design. The generator profile also differs from the streaming anomaly fixture.

| Simulated hours | Raw records | Features | Predictions | Sink wall s | Raw records/s | Python CPU s | Process lifetime peak RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 196 | 84 | 0 | 0.024215 | 8094.18 | 0.024803 | 23416 |
| 8 | 772 | 372 | 0 | 0.091277 | 8457.76 | 0.094812 | 26104 |
| 24 | 2308 | 1140 | 0 | 0.369196 | 6251.43 | 0.377829 | 30840 |

Every recorded persisted-invariant check was true for this scope. The RSS statistic is a cumulative process-lifetime high-water mark, not a separately reset per-case peak. These timings exclude Kafka, Spark, registry access and inference; they do not measure end-to-end streaming throughput or capacity.

The source identity is `working-tree-sha256:43b0dc43e2fa3365cae351991ed1057391e6bed236ba8df95b270d78a45f1bbc`, which differs from benchmark03. Original source-file hashes remain in the report. These results must not be pooled with the later streaming measurements or described as measurements of its final runtime source.
