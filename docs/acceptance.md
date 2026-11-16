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
| Rendered dashboard in a real browser | **Chrome 157 successfully rendered 3 hosted scenarios**: desktop 1440px, responsive 600px, and HTTP 503. After fixing observed narrow-header overlap, all 3 original PNGs and JSON reports were inspected. | [PR #8 / original GitHub run](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746); deliberately synthetic API fixture of the actual source HTML, **not** Kafka/Spark live UI or production accessibility acceptance |
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

## Hosted CircleCI unit verification — 15 November 2026

| Lane | Observed result | Boundary |
|---|---|---|
| Python build-and-test | **Success** at `579cbc5` in [pipeline #5](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/5?useNewPipelines=false) (1m14s) and `ce7674c` in [pipeline #7](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/7?useNewPipelines=false) (1m15s) | Compile, lint, pytest test-suite and JUnit output; CI assertion counts not independently read. Merged as [PR #4](https://github.com/mehdisafer/industrial-streaming-mlops/pull/4) at `b454451` |
| Container qualification | **Success reported by CircleCI** on exact `4bcc55e` in [`reference-containers`](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7); the old [pipeline #8](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/8?useNewPipelines=false) failed on `ce7674c` | GitHub's `ci/circleci: reference-containers` status is `success` at `4bcc55e`. The job runs real `scripts/integration.sh`. Detailed original artifacts are held by CircleCI and not independently rechecked in this merge |

The Python unit job alone never establishes Docker acceptance. The separate
Docker reference job reports success on `4bcc55e`. **CircleCI, not GitHub
Actions, is the required CI for P03 acceptance.** The maintainer reports that
GitHub Actions has exhausted its quota. This explanation is not independently
proven by the unavailable runner annotation, but GitHub Actions is a
non-blocking historical/optional CI path. Those **earlier** CircleCI unit/container
results do not qualify Databricks, rendered browser behavior or a public release.
The separately executed 16 November Chrome qualification below closes only the
synthetic rendered-dashboard gate.

## Hosted container execution — source-bound record

CircleCI's `ci/circleci: reference-containers` status is **success** for
[`4bcc55e`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/4bcc55ec470a752bac59dfa45f561dad0261d95e/checks); its [hosted job](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7) uses the pinned
Linux machine image `ubuntu-2404:2026.10.1`, validates Docker Compose and
executes the bounded `scripts/integration.sh` against real services.
The previous [#8 failure](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/8?useNewPipelines=false) is explained by an existing
`evidence/integration` directory and exit 64. Commit `9e6852d` moved host
metadata into `evidence/ci-host`, and `4bcc55e` added a routable ingress
bridge while keeping published host ports bound to loopback.

The branch's earlier hosted acceptance note **reports** a passing run receipt,
20 final checks, 4,380 raw rows, 2,092 features/scores, 12 alerts, model
lifecycle and recovery assertions, and scoped cleanup. Its four reported SHA-256
values are preserved below for later comparison with the original CircleCI
artifacts. These receipt-level details are **not independently byte-verified by
this reconciliation**; a successful job status is not a substitute for their
original contents.

| CircleCI artifact (reported) | Recorded SHA-256, not independently rechecked |
|---|---|
| `integration/run-receipt.json` | `78b45855d20b1ab5c6784f59eff21dd8e42f2c0dedc345f8383cfc19312d148a9` |
| `integration/final-verification.json` | `5026ba8908142f6e21ca83a1ff2a03be6be63d9df3bb6f348a02723463d4383dd` |
| `integration/lifecycle-assertions.json` | `c9d805ba5503796db98a688e9b593de147744d2019455fe11b8d659811eb3c0de` |
| `integration/recovery-assertions.json` | `66589288bdba4bb79b6068baab056fa822fa1545df851779f407ce62e7ec42512e` |

This hosted Docker reference remains single-host synthetic validation, not
Databricks acceptance, distributed scalability or production readiness.

## Hosted real Chrome dashboard qualification — 16 November 2026

**Scope:** source-bound visual qualification of the actual read-only
`src/industrial_mlops/dashboard.py` HTML page with explicitly synthetic JSON API
responses, not another Kafka/Spark/MLflow, Databricks or operational hardware run.

- GitHub source branch: [PR #8](https://github.com/mehdisafer/industrial-streaming-mlops/pull/8),
  tested implementation `2148c9f84fea93de1d4d065c9683cf04f5525ded`.
- [GitHub Actions run 43921124746](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746),
  job **Rendered dashboard (real Chrome)** succeeded with **Chrome 157.0.8103.50**.
  Its reviewed PR checkout was `d7ee5640139b9d3defa4900fcc768a979d9c72ab`,
  which identifies the PR merge checkout, not the source branch head.
- [Original browser artifact 14253962621](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746/artifacts/14253962621):
  all three JSON receipts, PNG screenshots, original `dashboard.py` SHA-256,
  Chrome version, source commit and failure-state evidence were downloaded and inspected.
- CircleCI `build-and-test` and `dashboard-browser` both reported **success**
  on the corresponding PR head in [workflow 541ae815](https://app.circleci.com/workflow/541ae815-63f8-4aec-b814-bb3e2554f941).
  The GHA browser logs and original artifacts were independently inspected;
  this is not a claim about all pending GitHub Actions integration jobs.

| Observed Chrome scenario | Width | Actual results | Screenshot SHA-256 |
|---|---:|---|---|
| Healthy dashboard desktop | 1440 px | Five metric cards, synthetic alert and prediction visible; no external resource, overflow or header overlap | `1787d7001d6f847b7ea1f06086db443f5f23ec3b06f94187848b4261353d31eb` |
| Healthy dashboard responsive | 600 px | Two-column metric layout, readable separated status badge, scrolling table constrained inside panel, no document-level overflow or badge overlap | `94d8f215a069630fcecec34e862b9afcbc878b1ef10ef76698888cd6399cc1c2` |
| Unavailable API (HTTP 503) desktop | 1440 px | Explicit `Pipeline unavailable` message; no false healthy state | `3041e000638b5974e80f6d40f9f0e4c02c9449213100813a21325ce30175d480` |

All three `*.json` receipt files report `passed: true`,
`horizontal_overflow: false`, `header_overlap: false` and
`external_resources: 0`. The actual dashboard source SHA-256 is
`794163773db059fc615057fd0b0be8d353fa2f316dd7696358dc511db6b1b433`.
The intentionally untrusted HTML-looking site identifier remained **literal text**
in the rendered table; no injected `<img>` element was created.

**Failure history remains visible:** the older local Chromium attempt was blocked
by sandbox socket restrictions; a first CircleCI capture failed on an assertion
that expected the complete feature ID despite the UI's existing 14-character
projection. A subsequent *successful* browser screenshot still showed a genuine
mobile header overlap not covered by the initial checks. The amended CSS and
actual DOM rectangle assertion passed on the final screenshots. The old failures
are not relabeled as passes.

Reproduce with `bash scripts/dashboard_browser_smoke.sh` from a supported
non-root Linux runner with sandboxed Chrome, an unused local socket and a **new**
`DASHBOARD_BROWSER_EVIDENCE` path. The script uses the original static HTML
source plus a synthetic, locally hosted data fixture, retains independent
screenshot/DOM hashes and does not modify the application's backend. No
`--no-sandbox`, provider credentials, external telemetry or deployment applies.

**This completes the specific synthetic browser rendering evidence gap.**
Keyboard/screen-reader accessibility, an attended operator assessment, source
data quality and browser behavior with a live Kafka-backed service remain
outside this isolated visual proof.

## Open qualification and release gates

| Gate | Current state | Evidence needed to close it |
|---|---|---|
| CircleCI container reference | **Job succeeded on source `4bcc55e`**; original artifact hashes/contents not independently rechecked in this merge | Download and retain source, image identities, Kafka/Spark/MLflow logs, HTTP/recovery and cleanup artifacts from the [successful job](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7); verify SHA-256 against the reported values |
| Databricks | **Open**; no authorized workspace run | Existing authorized compute/storage, Kafka access, provider run IDs, Delta versions and recovery evidence |
| Browser presentation on synthetic fixture | **Chrome 157 real rendered screenshots and DOM checks passed on 16 November 2026**, including corrected mobile header. Original local Chromium blocker remains historical. | Accessibility/operator evaluation and actual production-backed UI still require separate proof; see [hosted screenshots](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746/artifacts/14253962621) |
| Release publication | `v0.1.0-rc.1` notes prepared; no tag or GitHub release published | [PR #2](https://github.com/mehdisafer/industrial-streaming-mlops/pull/2) and [follow-up PR #3](https://github.com/mehdisafer/industrial-streaming-mlops/pull/3) record source review; tag/release publication requires its own verified action |
| License and visibility | Repository private; no owner-approved license | Owner decisions and publication review before any visibility change |
| Repository settings | Description is null and topics are empty; no supported metadata/release mutation tools. Rulesets read returned a plan/visibility 403; branch-protection read returned an integration-access 403 | Supported authorized administration and, for rulesets, a separate owner decision on the documented plan/visibility requirement |

The [historical check receipts](evidence/checks/README.md) preserve GitHub Actions
jobs that failed before runner allocation (`runner_id=0`, no executed steps).
The maintainer reports an exhausted GitHub Actions quota. The annotation is
inaccessible, so quota exhaustion is not independently verified from those
jobs. This historical failure is not an application test failure or a P03 gate.
CircleCI is the accepted CI alternative. No plan upgrade, quota restoration,
public-visibility change or GitHub Actions rerun is required or implied.

Historical GitHub Actions CI scope selection was locally exercised with real
temporary Git history: documentation-only updates skip heavy integration,
source changes select it, and manual dispatch selects the full path.
Job-level concurrency behavior is implemented and statically checked but
**hosted GitHub Actions scheduling was not executed**; this is not a P03 blocker
because CircleCI is the designated hosted CI. No paid runner or cloud resource
was provisioned or requested.

## Prioritized follow-up

1. Preserve and independently check CircleCI Docker job artifacts from the
   successful source `4bcc55e`. The earlier exit-64 failure is diagnosed;
   record the exact correction and retained evidence, not a fabricated rerun.
   GitHub Actions runner allocation/quota is **out of P03 acceptance scope**.
2. Run the [Databricks qualification](databricks.md) on explicitly authorized existing
   resources, including library/access checks, business-output parity, recovery and
   available consumption data. Local Spark is not provider qualification.
3. Complete independent operator/accessibility review and the owner license/publication decisions. The source-bound real Chrome rendering gate is now supported by separate executed evidence.
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
