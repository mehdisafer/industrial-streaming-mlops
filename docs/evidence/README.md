# Recorded execution evidence

**The local reference passed the recorded native protocol. Full P03 acceptance
remains open for Databricks and other explicitly listed release gates, not for
GitHub Actions runner allocation.** CircleCI is the designated hosted CI.
Its Python unit lane and separate Docker reference job succeeded on specified
source commits, but Docker artifact byte verification remains open. All fleet data is
synthetic. No industrial production reliability or distributed capacity is claimed.

## Source-bound hosted evidence — 17 November 2026

The [licensed-main original artifact audit](hosted-reference-2026-11-17/README.md)
independently rehashed original GitHub Actions archives from source `185c415`:
**333 Python tests passed**, **six persisted business checks passed** on a real
Docker-hosted Kafka/Spark/MLflow chain, **2,513 raw records, 1,225 predictions,
7 alerts**, and completed rollback/cleanup. A separate source-security report
recorded zero matched patterns and zero published vulnerabilities among 123
entries, with four licence metadata flags requiring explicit upstream review.
This is additional evidence; CircleCI remains the selected CI and its older
job #13 artifact bytes remain unverified. Databricks remains open.

## Current follow-up — 15 November 2026

[The follow-up evidence](followup/README.md) binds to source `daaa5b8`: **294 tests**,
**21 real native checks**, **28 real restored-backlog checks** and **13 actual
JavaScript assertions with explicit DOM/fetch doubles**. It includes original
receipts, current advisory checks, the twelve-commit incremental history review,
plus the browser startup limitation and historical GitHub Actions runner failures. Six hundred seconds were declared for the
restoration exercise; it completed in 95.825 s. The fresh native fault/rollback
execution completed in 200.955 s.

The older detailed results below remain historical evidence from the original
delivery. They are not relabelled with the new execution commit or timing.

## Read the evidence by scope

| Scope | Observed result | Original evidence |
| --- | --- | --- |
| Current application correctness | 333 tests passed on `185c415`; earlier 294 tests passed on `daaa5b8` | [Original hosted JUnit and file hashes](hosted-reference-2026-11-17/README.md), [earlier local suite](followup/README.md) |
| Current native recovery and rollback | 21 checks passed on reviewed source `daaa5b8` | [Fresh native receipts](followup/native/qualification.json) |
| Restored retained backlog and fresh input | 28 checks passed; 303 retained + 303 fresh records, 1,965 final predictions | [Restoration receipts](followup/restore/restore-qualification.json) |
| Original application checks | 110 tests passed; Ruff passed | [Historical checks and JUnit](checks/README.md) |
| Model evaluation and registry | Real HTTP MLflow registration; frozen validation-selected thresholds; both models met recorded gates | [Model report and all holdout rows](model/README.md) |
| Real Kafka → Spark → MLflow → dashboard | 21 native checks passed, including actual source lag | [Native run](native/README.md) |
| Faults and restoration | Before/after-commit recovery; active broker outage; new-input model rollback; restored Kafka/MLflow/checkpoint | [Native receipts](native/qualification.json) |
| Partitions, worker threads and alert invariants | Four real variants passed; identical fixed-input business results | [Streaming benchmark](benchmark/README.md) |
| Current dependency review | 123 dependency entries audited on `185c415` with no known advisories reported; four licence metadata flags have version-bound upstream source review but no complete distribution clearance | [Original checks archive](hosted-reference-2026-11-17/README.md), [four upstream licence sources](third-party-licensing-2026-11-17/README.md), [prior 15 November audit](followup/checks/dependency-audit.json) |
| Earlier failures | Original failed/intermediate attempts and corrective scope retained | [Attempt history](attempts/README.md) |
| CircleCI Python unit CI | `build-and-test` succeeded on `579cbc5` and `ce7674c`; CI JUnit assertion counts not independently read | [CircleCI #5](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/5?useNewPipelines=false), [#7](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/7?useNewPipelines=false), [PR #4](https://github.com/mehdisafer/industrial-streaming-mlops/pull/4) |
| Docker container qualification | CircleCI `reference-containers` succeeded on `4bcc55e`; separate real GitHub Actions Docker integration succeeded on `185c415` with **original ZIP bytes rehashed** | [Current-source original Docker receipts](hosted-reference-2026-11-17/README.md); [historical CircleCI artifact boundary](circleci/README.md) remains unverified |
| Historical GitHub Actions (non-gating) | Runner ID 0, no steps; maintainer reports quota exhausted. CircleCI replaces it for P03 acceptance; provider annotation was not independently retrieved | [Original runner evidence](checks/README.md#ci-helpers-and-hosted-execution) |
| Databricks | Bundle and adapter prepared; no authorized provider run | [Provider prerequisites](../databricks.md) |

## Native source and output reconciliation

Native08 ran in a clean Python 3.11.18 environment with real JVM Kafka 3.9.2,
Spark 3.5.9, Java 17 and an HTTP MLflow 3.17.2 server. It reused the stopped native01
model state only after matching all seven training-source files. It did not retrain
or tune a threshold on its final data. Its source and runner identities are in the
original report.

| Final source disposition | Records |
| --- | ---: |
| Accepted sensor events | 2,164 |
| Accepted labels | 2,164 |
| Control heartbeats | 24 |
| Explicit rejections | 24 |
| Duplicate events | 4 |
| **Raw Kafka records** | **4,380** |

The run persisted **2,092 features and 2,092 predictions**, with **12 emitted alerts**,
no pending predictions and observed lag **zero on every partition**. Alerts are
emissions, not an assertion that every alert is correct; held-out quality is evaluated
separately below. The ledger, source receipts and restored broker offsets reconcile.

Kafka was stopped while the streaming query was active. The measured stop-to-restart
interval was **15.379 seconds**; the same active worker consumed **303 new recovery
records**. Candidate activation and rollback each processed new telemetry with the
recorded numerical model version. Registry aliases and local deployment state remain
separate operations.

The stopped-state restoration reopened the database, restarted Kafka and MLflow,
reloaded an artifact and checked its prediction, then resumed the relocated copied
checkpoint without duplicate business rows. That last step resumes a caught-up
checkpoint with zero new rows; it is not a separate test of a non-empty recovery
backlog after storage loss.

The full native sequence took **202.0 seconds**, excluding the reused model training.
Its per-prediction inference p50/p95/p99 was **0.274 / 0.698 / 0.901 ms**. Measured
Kafka-timestamp-to-prediction p50/p95/p99 was **29.462 / 30.223 / 30.298 seconds**,
including the intentional registry outage and fresh worker starts. These timings
are distinct from simulated failure lead time.

## Held-out model result

The final held-out dataset contains **5,166 labeled features, 24 failures and
17.9375 machine-days**. Training, validation and test are disjoint generated datasets
ordered in time, with labels mature before the next split. The 30-minute cooldown and
the operating threshold were fixed before final-test scoring.

| Metric | Explainable baseline | Logistic-regression candidate |
| --- | ---: | ---: |
| Threshold | 0.500 | 0.975 |
| False alerts | 29 | 1 |
| False alerts per machine-day | 1.6167 | 0.05575 |
| Useful failure recall | 87.5% | 100% |
| Alert precision | 42% | 96% |
| Median simulated actionable lead | 919.36 s | 962.61 s |

Useful recall requires at least five minutes between feature availability and the
simulated failure. Scores are not calibrated probabilities. These controlled
synthetic results do not establish generalization to real equipment or unseen sites.
The [model card](../model-card.md) defines event and alert metrics separately.

## Workload and concurrency result

The final real comparison used four machines, one cold JVM per case and a
200-offset trigger bound. It completed in **84.6 seconds** within its predeclared
600-second budget. Each 24-hour case used the same input bytes and produced **1,141
predictions, four alerts and 18 cooldown suppressions** from 22 above-threshold
predictions. Features, scores, alert identities, sensor evidence and cooldown
decisions matched across partitions and worker threads.

| Synthetic hours | Kafka partitions | Spark local threads | Raw records | Worker wall time | Raw records/s, including startup |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 | 1 | 1 | 205 | 13.29 s | 15.42 |
| 24 | 1 | 1 | 2,317 | 16.18 s | 143.18 |
| 24 | 3 | 1 | 2,317 | 18.96 s | 122.23 |
| 24 | 3 | 2 | 2,317 | 18.65 s | 124.27 |

There is one sequential observation per case, shared service/OS cache and a bounded
single-writer driver sink. The measurements establish this protocol's behavior;
they do not establish a statistical speedup, steady-state saturation limit or
multi-node scale. The benchmark includes preloaded backlog and drainage. Its
resource report distinguishes sampled process-tree RSS, CPU deltas and runner CPU;
RSS sums can count shared pages more than once. No provider billing data was available.

The earlier 2/8-hour comparison also passed feature/score parity, but emitted zero
alerts. It is retained as prior evidence; the final 24-hour protocol was added to
exercise nonzero alerts and cooldown suppression without changing the model.

## Binding executions to the delivered source

| Execution or review | GitHub commit |
| --- | --- |
| Native08 complete protocol | [`56bf437`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/56bf4370bf0b1835b59da815006216976e19e5a7) |
| Final 2/24-hour streaming benchmark | [`349ec4c`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/349ec4c892c15b0deeeb34c81a2cc8baeecbf656) |
| Reviewed executable source, including final CI controls | [`199b53a`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/199b53a5025bd309b6fe8de81d5c1888ac5b09b1) |
| 15 November native and restored-backlog executions | [`daaa5b8`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/daaa5b8cc165b112c6cdce431fa1a8666c05892c) |

For the original merged delivery at `6c9f33f`, application runtime, contracts,
configuration and dependency bytes matched native08. Later changes within that
original delivery extended the benchmark wrapper and corrected CI selection/concurrency.
Its [source binding](checks/release-source-binding.json)
matches all 54 executable/test/deployment/dependency files to fetched GitHub blob
identities. Evidence and documentation are committed afterward without modifying that
runtime. Original training identity remains unchanged in its report; a later commit
has not been substituted into an earlier observation. The follow-up PR changes the
producer, native runtime profile, dashboard recovery and provider adapter. The
[new binding](followup/checks/source-binding.json) and fresh service receipts qualify
their current local execution scope; they do not inherit a provider or browser pass.

Each evidence directory contains copy/compression manifests. Input archives decompress
to the original bytes, which were checked before compression. Hashes prove content
correspondence; they do not certify authorship, security, model fitness or vendor
integration. The complete project remains tracked in [issue #1](https://github.com/mehdisafer/industrial-streaming-mlops/issues/1).
