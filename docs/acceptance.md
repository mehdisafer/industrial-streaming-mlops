# Acceptance and remaining work

I distinguish implemented behavior, executed checks and acceptance within a named
environment. The [evidence index](evidence/README.md) preserves commands, source/model
identities, inputs, original outputs and earlier failures. Native integration,
container qualification and Databricks qualification have separate gates.

## Observed local acceptance

| Criterion | Observed result | Evidence and limit |
|---|---|---|
| Contracts, causal features, delayed labels, sink atomicity, provider guards and monitoring | **294 tests passed in 19.29 s**, zero failures/errors/skips; Ruff passed | [Current receipts](evidence/followup/README.md); named predictor/client doubles are unit evidence |
| Kafka → Spark → MLflow → persisted outputs | Current reviewed source: **all 21 checks passed** in 200.955 s | [Fresh native result](evidence/followup/native/qualification.json); real single-host services at `daaa5b8` |
| Source reconciliation | 4,380 raw = 2,164 sensors + 2,164 labels + 24 heartbeats + 24 rejects + 4 duplicates | Native receipt; 2,092 features/predictions, 12 alerts, no pending work, zero observed lag in all three partitions |
| Worker/broker recovery and unavailable model | Before/after-commit recovery, retained pending predictions and active-query broker recovery observed | Broker stop-to-restart interval 15.103 s; active worker consumed 303 new recovery records |
| Model deployment and rollback | Candidate and restored baseline scored subsequent real stream input | Native deployment/prediction receipts; numeric activation is separate from the registry alias |
| Stopped-state restoration with continued work | **28/28 checks** in 95.825 s: 303 retained messages and 303 fresh post-restore messages consumed | [Restoration report](evidence/followup/restore/restore-qualification.json); 4,077 raw records, 1,965 predictions, six preserved alerts, immutable history, four worker runs, zero lag and observed owned-process cleanup; cross-host recovery remains untested |
| Fixed-input business invariance | Three 24-hour variants produced identical features, scores, alert business fields and cooldown outcomes | [Benchmark03](evidence/benchmark/README.md); each has 2,317 raw records, 1,141 predictions, 4 alerts and 18 cooldown suppressions |
| Bounded load/resource observation | Four 2-hour/24-hour variants completed in 84.614 s overall | Worker wall times include startup/shutdown; CPU/RSS and latency receipts are scoped observations, without statistical speedup or multi-node capacity claims |
| Model quality gates | Baseline and candidate eligible on the original synthetic holdout | [Model evidence](evidence/model/README.md); 5,166 rows, 24 failures, 17.9375 machine-days |
| Dashboard data and script behavior | Real HTTP HTML/JSON; **13 JavaScript assertions** including HTTP-error recovery, six restored alert rows and 40 prediction rows | [Current check scope](evidence/followup/README.md#dashboard-and-unresolved-environment-gates); explicit minimal DOM/fetch doubles, no browser rendering or screenshot acceptance |
| Python dependency advisories | 122 third-party packages audited; zero known advisories reported on 15 November | [123-entry report](evidence/followup/checks/dependency-audit.json) explicitly skips the local project, which is not indexed on PyPI; limited to the recorded dependency/database state |

The baseline emitted 29 false alerts (1.6167 per machine-day), with 87.5% useful
failure recall and 42% alert precision. The candidate emitted one false alert
(0.055749 per machine-day), with 100% useful recall and 96% alert precision. Median
true-alert lead was 919.36 s and 962.61 s respectively. These are cooldown-aware
operational metrics on simulated feature-availability time. The validation-selected
thresholds, per-event metrics, warm model-only latency and qualification policy remain
in the original report; they are not interchangeable with measured pipeline latency.

For the original delivery, training completed during native01, whose aggregate integration attempt
failed later. Native08 reused that unchanged training result. Native07's lag metric
was unavailable and is superseded by native08's explicit zero-lag check. The
[attempt history](evidence/attempts/README.md) retains these distinctions. Source
manifests and binding receipts identify executed bytes; later CI/documentation changes
are not represented as having run inside an earlier qualification. The 15 November
follow-up trained the same fixed protocol in the restored-backlog exercise, then
reused that stopped model state for the fresh native fault/rollback run after
checking its training source. Both new service scenarios bind to `daaa5b8`; their
receipts are separate from native08 and the original model/benchmark evidence.

The application suite is reproducible with:

```bash
uv run --frozen pytest -q -m 'not integration'
uv run --frozen ruff check src tests scripts databricks
```

## Open qualification and release gates

| Gate | Current state | Evidence needed to close it |
|---|---|---|
| Container profile | **Not executed**; Docker unavailable | Fresh-project build/smoke, resolved image identities, service logs and scoped cleanup receipts |
| Hosted GitHub Actions | [Source run 43754981478](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43754981478): no executed steps, runner ID 0, one unreadable annotation; integration skipped | Read the annotation through supported GitHub access, resolve its actual cause, then obtain hosted checks/integration on the reviewed source; no green status inferred from local tests |
| Databricks | **Open**; no authorized workspace run | Existing authorized compute/storage, Kafka access, provider run IDs, Delta versions and recovery evidence |
| Browser presentation | Fresh Chromium launch on 15 November blocked by `socket(): Operation not permitted`; [original receipt](evidence/followup/checks/browser/startup.json) | Real browser layout/interaction review and captured output; minimal-DOM checks do not close this gate |
| Release publication | `v0.1.0-rc.1` notes prepared; no tag or GitHub release published | [PR #2](https://github.com/mehdisafer/industrial-streaming-mlops/pull/2) and [follow-up PR #3](https://github.com/mehdisafer/industrial-streaming-mlops/pull/3) record source review; tag/release publication requires its own verified action |
| License and visibility | Repository private; no owner-approved license | Owner decisions and publication review before any visibility change |
| Repository settings | Description is null and topics are empty; no supported metadata/release mutation tools. Rulesets read returned a plan/visibility 403; branch-protection read returned an integration-access 403 | Supported authorized administration and, for rulesets, a separate owner decision on the documented plan/visibility requirement |

The [check receipts](evidence/checks/README.md) preserve the distinct GitHub responses.
The rulesets restriction does not establish the cause of the Actions job failures.
No plan upgrade or public-visibility change is assumed, even though the repository
readback reports the user's administrative permission.

CI scope selection was tested with real temporary Git history: documentation-only
updates skip the heavy job, source changes select it, and manual dispatch selects the
full path. Job-level concurrency preserves an active integration and bounds its group
to one pending job. These are locally tested selection logic and statically checked
configuration; hosted scheduling remains open. No paid runner or cloud resource was
provisioned to close a gate.

## Prioritized follow-up

1. Execute the container and hosted-CI gates on an available authorized runner for
   the reviewed source. Preserve original failures and receipts.
2. Run the [Databricks qualification](databricks.md) on explicitly authorized existing
   resources, including library/access checks, business-output parity, recovery and
   available consumption data. Local Spark is not provider qualification.
3. Complete browser presentation review and the owner license/publication decisions.
   Apply prepared description/topics/protection only through a supported authorized
   administration path; verify anonymous access only after a visibility change.
4. Before unattended local operation, define database/evidence retention, archival,
   disk limits, service supervision and recovery objectives. The current finite demo
   profile and Kafka retention setting do not implement complete local retention.
5. Before larger deployment, implement distributed durable feature state, multi-node
   recovery tests and authenticated service exposure. SQLite remains a single-writer
   boundary, and adding local Spark threads does not remove it.
6. Evaluate representative external telemetry and unseen sites, with calibrated scores,
   uncertainty estimates and maintenance costs, before claiming field predictive value.
