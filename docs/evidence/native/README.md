# Completed native qualification: attempt 08

This folder preserves the original delivery's execution. The
[15 November follow-up](../followup/README.md) adds a fresh fault/rollback run on
the updated source and a separate restored-backlog/fresh-input exercise. Original
receipts below remain unchanged.

[qualification.json](qualification.json) is the original result of native attempt **08**: `passed: true`, with **all 21 checks true**. This run used an actual single-host JVM Kafka broker, local Spark Structured Streaming and an HTTP MLflow server, with Python 3.11.18 and generated sensor data. The initial workload covers six machines over 24 simulated hours, followed by separate recovery, candidate and rollback inputs. It is a local integration qualification, with no distributed scale or Databricks execution claimed.

## Final observed state

| Measure | Observed value |
| --- | ---: |
| Raw Kafka records | 4,380 |
| Accepted sensor events | 2,164 |
| Accepted delayed labels | 2,164 |
| Control records | 24 |
| Duplicate source records | 4 |
| Rejected source records | 24 |
| Features | 2,092 |
| Predictions | 2,092 |
| Alerts | 12 |
| Committed batch-ledger entries | 7 |
| Pending events / predictions | 0 / 0 |
| Observed source lag | 0 offsets across three partitions |

The source dispositions balance: 2,164 sensor events + 2,164 labels + 24 control records + four duplicates + 24 rejects = 4,380 raw records. The original report records database integrity and referential checks separately. Broker end offsets and sink next offsets agree for all three partitions. Total qualification elapsed time was 201.998 seconds, including service startup, injected failures, broker interruption and restoration.

## Checks recorded by this run

| Check | Original recorded value |
| --- | --- |
| `active_broker_outage_recovery` | true |
| `active_worker_consumed_recovery_input` | true |
| `after_commit_recovery` | true |
| `all_source_receipts_balance` | true |
| `batch_stream_feature_parity` | true |
| `batch_stream_prediction_parity` | true |
| `before_commit_recovery` | true |
| `candidate_scored_real_stream` | true |
| `dashboard_reads_real_sink` | true |
| `drift_does_not_promote` | true |
| `final_invariants` | true |
| `final_source_lag_zero` | true |
| `kafka_log_restore` | true |
| `mlflow_artifact_restore` | true |
| `model_recovery_scores_pending` | true |
| `pipeline_database_restore` | true |
| `restored_checkpoint_resume` | true |
| `rollback_scored_real_stream` | true |
| `source_count_matches_producer` | true |
| `source_unchanged_during_run` | true |
| `unavailable_model_retains_features` | true |

The [dashboard snapshot](dashboard-snapshot.json), [HTTP response](dashboard-response.html), per-worker JSON receipts and logs are copied unchanged. The [console log](console.log) is the original top-level execution log. Before-commit and after-commit failures were intentional; their failure receipts and recovery logs are retained.

## Active broker interruption

The [active-broker-outage receipt](active-broker-outage.json) records the broker port closed while a Spark query was active. Its stop-to-restart interval was **15.379 seconds**. A distinct recovery publication acknowledged **303 new records**, and `active_worker_consumed_recovery_input` establishes that the active worker consumed that new input after the broker returned. This is a measured local recovery scenario, not a failover or availability guarantee.

## Restoration evidence and scope

The runner backed up the stopped local deployment and exercised restored SQLite state, Kafka logs, MLflow artifacts and the Spark checkpoint. The original [backup manifest](backup-manifest.json) covers **181 files / 51,731,181 bytes** and records quiescence as `operator_asserted_stopped`. The complete binary backup is not included in this evidence bundle.

The run checked reopened pipeline state, observed broker offsets, reloaded the registered MLflow model and checked a prediction, then explicitly relocated the checkpoint binding and resumed it. [restored-checkpoint-run.json](restored-checkpoint-run.json) reports the same final counts, observed lag zero and **zero new input rows**, with no duplicate sink effects. That establishes restart of this already caught-up checkpoint; it does not test a new publication after restoration or restoration across hosts. The distinct restored [Kafka log](restored/kafka.log), [MLflow log](restored/mlflow.log) and [worker log](restored/restored-worker.log) preserve the actual service restart observations.

## Timing contexts

| Observation | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Runtime inference, milliseconds | 0.274 | 0.698 | 0.901 |
| Broker-to-prediction, seconds | 29.462 | 30.223 | 30.298 |

The runtime inference measurement includes DataFrame conversion. Broker-to-prediction timing includes the intentional registry outage and pending-prediction recovery; it is not a steady-state service latency benchmark. The separate [model evaluation](../model/README.md) measures warm model-only calls and evaluates the predeclared inference gate. These measurements have different boundaries and should not be interchanged. Synthetic failure lead times use event/feature-availability timestamps, not wall-clock deployment lead.

## Executed source and model lineage

The recorded runtime source commit is `56bf4370bf0b1835b59da815006216976e19e5a7`, with source identity `working-tree-sha256:8756108fa9d3f07592b2a40e51f9da1a07baa2314bda77e413af08b23d930866`. The [original source manifest](source-manifest.json) and the runner hash in the qualification report identify the executed bytes.

The [source-binding receipt](source-binding.json) binds 54 executable, configuration, dependency, deployment and test files to commit `55a082ea7d72818cc8088f70bcd4710d0decb9dd`. The six-commit history audit confirms that the change from `56bf437` to `55a082e` affects only `.github/workflows/ci.yml`; all 62 other blob paths are unchanged. Runtime and dependency bytes are therefore identical between those commits. This does not claim that native08 executed the later CI workflow.

Native08 reused the [original native01 training report](../model/training-report.json), with no new training run. Its full embedded training report is identical, and all **seven training-source file hashes remain unchanged**. The model code identity remains `working-tree-sha256:0198d952137fe7439ed7818dbae0fbb81074f53e8e8da0032f365a2acab1db53`; it has not been replaced with the later runtime commit identity.

## Original replay inputs

| Compressed original | Records / acknowledged publication | Uncompressed bytes |
| --- | ---: | ---: |
| [initial.jsonl.gz](inputs/initial.jsonl.gz) | 3,471 | 1,720,481 |
| [broker-recovery.jsonl.gz](inputs/broker-recovery.jsonl.gz) | 303 | 148,881 |
| [candidate.jsonl.gz](inputs/candidate.jsonl.gz) | 303 | 148,865 |
| [rollback.jsonl.gz](inputs/rollback.jsonl.gz) | 303 | 148,874 |

Each gzip has `mtime=0` and no filename header. Decompression was checked byte-for-byte against the original JSONL; hashes, sizes and repeat-compression checks are recorded in [bundle-manifest.json](bundle-manifest.json). These are generated inputs, including deliberate invalid and duplicate cases, rather than customer measurements. The [input scan receipt](input-secret-scan.json) records inspection of all four original uncompressed files with the repository's existing `inspect_text` rules: zero skips and zero findings. This is a bounded pattern scan, not a guarantee against every possible secret.

## Earlier attempts and evidence integrity

[Attempt 07](../attempts/07-superseded-note.md) originally reported 19 checks true, but its lag observations were unavailable with a `TypeError`, and its report did not require a final zero-lag check. Its original report and logs remain unchanged. Attempt 08 supersedes it with the corrected lag observation and the additional active-worker input-consumption check. Earlier failed or blocked attempts remain in [the attempt history](../attempts/README.md).

Every original file from native08's evidence directory is included unchanged. The manifest identifies each exact-copy source and destination SHA-256, the three distinct restored logs, the original backup manifest and the additional source-binding receipt. All copied files are below 2 MiB each. Runtime originals and source files were not modified during packaging.
