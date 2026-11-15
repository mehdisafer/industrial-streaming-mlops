# Follow-up qualification — 15 November 2026

The reviewed local reference now has a real restored-backlog exercise and a fresh
fault/rollback execution on [`daaa5b8`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/daaa5b8cc165b112c6cdce431fa1a8666c05892c).
The [follow-up PR](https://github.com/mehdisafer/industrial-streaming-mlops/pull/3)
continues the original delivery. Container execution, hosted CI, Databricks and
browser presentation remain separate open gates.

## Results and original receipts

| Scope | Observed result | Original evidence |
|---|---|---|
| Application suite | **294 tests passed**, zero failures/errors/skips, in 19.29 s; Ruff passed | [JUnit](checks/pytest.xml), [test output](checks/pytest.log), [Ruff](checks/ruff.log) |
| Real restored backlog and new input | **28/28 checks**, 95.825 s within the predeclared 600 s budget | [Restoration report](restore/restore-qualification.json), [protocol](restore/protocol.json), [console](restore/console.log) |
| Real faults, model recovery and rollback | **21/21 checks**, 200.955 s inside the 480 s outer command bound | [Native report](native/qualification.json), [console](native/console.log) |
| Dashboard JavaScript | **13/13 assertions** with explicit DOM/fetch doubles and real saved HTTP responses | [Checker](checks/browser/checker.js), [original output](checks/browser/dashboard-js-console.json) |
| Python advisories | **122 third-party packages audited**, zero known findings; the local project was skipped | [123-entry report](checks/dependency-audit.json), [output](checks/dependency-audit.log) |
| New PR history | **12 commits / 34 distinct changed blobs / 18 paths**, exact byte correspondence and zero matched secret patterns | [Incremental history review](checks/pr-history-review.json) |
| Source identity | All **223 fetched source/evidence files** matched the execution tree before this evidence/documentation update | [Source binding](checks/source-binding.json) |

Python 3.11.18, Kafka 3.9.2, Spark 3.5.9, Java 17.0.21 and MLflow 3.17.2 were used.
The package lock was installed without changes. Kafka/JDK archives and the Spark
connector JARs were verified against the checked-in hashes. The JAR download
helper ran in the preliminary Python 3.12 environment; service execution and the
complete application suite used Python 3.11.18.

The advisory report contains 123 entries, including the local project, which is
not indexed on PyPI and could not be audited by that service. Its skip is retained
explicitly; zero known findings applies to the 122 packages that were checked.

## What changed

The earlier PR commits already implemented Kafka metadata readiness, producer
timeout consistency, bounded native JVM ergonomics, MLflow server job opt-out,
container inventory capture and provider recovery guards. This continuation
preserves those changes and adds three corrections established by review:

- **Kafka rate targets:** Spark can legitimately exceed `maxOffsetsPerTrigger`
  when it rounds small nonempty partition shares. The provider adapter now checks
  the separate hard driver row cap, retains raw rows on overflow and records the
  target and observed count. Reducing a rate target does not shrink a failed
  batch already recorded in a checkpoint.
- **Registry compatibility:** the reviewed provider loader requires native
  Databricks tracking plus Unity Catalog. Workspace registries and external
  tracking are rejected before MLflow access because their artifact-source
  layouts do not satisfy the current provenance contract. The
  [provider runbook](../../databricks.md) explains the limitation and its sources.
- **Restored history:** comparison now includes labels, accepted-event payloads,
  deduplication fingerprints, configuration and deployments. A same-count label
  corruption was invisible to the previous snapshot and structural verifier.
  Seven added SQLite regressions distinguish corruption from legitimate mutable
  processing progress. Seven additional provider cases cover the two API issues.

Review was performed by agents within the same assisted development process.
It is not independent human approval or provider acceptance.

## Restored backlog and fresh-input proof

Six synthetic machines at two sites first produced 24 hours of telemetry. After
the initial stream drained, another two hours were published and deliberately left
unconsumed. The runner observed its original owned processes stopped and the
service ports closed before copying the whole stopped deployment.

The verified copy was restored into a new directory. Only the copied checkpoint's
database path was relocated after checking its original binding. The original
database and checkpoint were preserved. Kafka and MLflow restarted from restored
state; the same numerical model version, source run, threshold, feature signature
and artifact digest were verified before processing continued.

| Stage | Raw records | Predictions | Alerts | Pending predictions |
|---|---:|---:|---:|---:|
| Initial stream | 3,471 | 1,711 | 6 | 0 |
| Restored retained backlog: **303 additional records** | 3,774 | 1,838 | 6 | 0 |
| New post-restore input: **303 additional records** | 4,077 | 1,965 | 6 | 0 |
| Repeated empty checkpoint resume | 4,077 | 1,965 | 6 | 0 |

Final dispositions reconcile exactly: **2,019 accepted sensors + 2,019 labels +
18 controls + 18 rejects + 3 duplicates = 4,077 raw records**. All three partitions
had observed lag zero. The two added short windows created new predictions but
no additional alerts; the six pre-existing alerts remained unchanged.

One Spark query identity survived four distinct worker run identities. Progress
offset ranges, producer acknowledgements and sink cursors establish unique source
coverage. Raw `numInputRows` metrics are retained unchanged and are not treated as
unique input counts: `foreachBatch` can read the same input more than once.
Batch/stream features and predictions matched, old business rows were preserved,
and the final empty resume produced no duplicate business effects.

All four Spark JVMs reported two available processors under the declared
`ActiveProcessorCount=2` profile. This controls JVM ergonomics; it is not an OS CPU
quota or a limit on every thread. Original resource samples and summaries are
retained alongside the stream receipts. The runner observed cleanup of its owned
process groups and closure of ports 5000, 8080, 19092 and 19093.

This is a same-host stopped-state recovery proof. Cross-host recovery, recovery
after Kafka retention has expired, and a distributed deployment remain untested.

## Fresh fault and rollback execution

The native scenario reused the just-trained, stopped MLflow state only after
matching the training-source manifest. It did not refit the model or adjust a
threshold on the final stream. Its original training report is preserved in
[training/training-report.json](training/training-report.json); all held-out rows
are retained in the [lossless CSV archive](training/holdout-predictions.csv.gz).

The current source passed model-unavailable retention/recovery, failures before
and after the sink commit, an actual broker interruption during a running Spark
query, candidate activation and compatible rollback with new telemetry.
The broker stop-to-restart interval was **15.103 s**; the same active worker
subsequently consumed **303 new recovery records**. See the
[active outage receipt](native/active-broker-outage.json).

The final native ledger contains **4,380 raw records = 2,164 sensors + 2,164 labels +
24 controls + 24 rejects + 4 duplicates**, **2,092 predictions** and **12 alerts**.
Pending work is zero and source lag is zero in every partition. The dashboard
returned real HTTP responses from this sink. The command returned exit zero;
[subsequent port observations](checks/post-native-ports.json) are recorded separately
from the stronger owned-process cleanup checks in the restoration runner.

| Measured latency | p50 | p95 | p99 |
|---|---:|---:|---:|
| Model inference, ms | 0.276 | 0.731 | 0.969 |
| Kafka timestamp to prediction, s | 26.322 | 27.082 | 27.162 |

The second row includes the intentional registry outage and worker startup. These
are this recovery protocol's observations, not steady-state service objectives.
The earlier partition/thread benchmark remains historical evidence; it was not
rerun or relabelled as a new scalability result.

## Dashboard and unresolved environment gates

The actual inline JavaScript rendered five metric cards, six alerts and 40 recent
predictions from the restored sink's saved HTTP response. A forced HTTP 503 set
the error state; the next successful refresh cleared the stale class and restored
the same rows without duplication. These are explicit minimal-DOM/fetch tests.

A fresh real Chromium launch failed before page creation with
`socket() failed: Operation not permitted` and SIGABRT. The
[original startup receipt](checks/browser/startup.json) is retained. No screenshot,
browser layout, CSS or accessibility result is claimed.

The [source GitHub Actions run](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43754981478)
failed before execution: `checks` had runner ID 0, an empty runner name and no steps;
integration was skipped. One annotation exists, but the connector rejects its
endpoint. Original [job](checks/github-source-jobs.json),
[check](checks/github-source-checks.json) and
[annotation-access](checks/github-annotation-access.json) responses are retained.
The cause is unknown; no billing or quota explanation is inferred.

Docker/Podman and a Docker daemon socket were unavailable. No authorized
Databricks workspace/compute/storage or provider run was supplied. These limitations
remain open in the [acceptance register](../../acceptance.md).

## Reproduce the evidence

Use fresh work directories and the repository's pinned bootstrap. The default
Python version is 3.11; the exact commands used are in the source binding.

```bash
export MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true
uv sync --frozen --python 3.11.18
bash scripts/bootstrap_native.sh
uv run --frozen python scripts/fetch_spark_jars.py
GITHUB_SHA=$(git rev-parse HEAD) uv run --frozen python scripts/qualify_restore.py \
  --work-dir state/restore-replay-001 --machines 6 --budget-seconds 600
GITHUB_SHA=$(git rev-parse HEAD) timeout --signal=INT --kill-after=45s 480s \
  uv run --frozen python scripts/qualify_native.py \
  --work-dir state/native-replay-001 --machines 6 --hours 24 --training-hours 72 \
  --reuse-training-from state/restore-replay-001
uv run --frozen pytest -q -m 'not integration'
uv run --frozen ruff check src tests scripts databricks
node docs/evidence/followup/checks/browser/checker.js \
  docs/evidence/followup/restore/dashboard-response.html \
  docs/evidence/followup/restore/dashboard-snapshot.json \
  state/dashboard-js-replay.json
```

The preliminary Python 3.12 provider check encountered two overlay mismatches:
the universal lock selects SciPy 1.18.1 there, while the provider's Python 3.11
overlay requires 1.17.1. The final 294-test run used the declared Python 3.11.18
profile, with no dependency pins changed to hide that distinction. The exploratory
3.12 console output is not an original receipt in this bundle.

## Provenance and review limits

The [copy manifest](copy-manifest.json) records original and stored hashes for
every copied receipt and losslessly compressed log/input. No original output was
rewritten as a pass. Databases, Kafka binary segments and executable model artifacts
are not published here. Hashes establish byte correspondence, not certification.

The incremental review covers all twelve new PR commits and their 34 changed
blobs, plus current issue/PR text and the empty comment/review collections observed
at review time. The original main-history review remains in
[the earlier check evidence](../checks/README.md). Pattern matching is bounded and
does not prove the absence of every possible secret.

Earlier PR commit messages mention restore01–restore07 attempts. Their original
run directories and reports were not present in the fetched Git history or supplied
files. The history review records this absence; counterexample fixtures in tests
are not substituted for those original executions. This bundle preserves the new
successful runs and the current blocked browser/hosted-CI observations.
