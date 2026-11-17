# Acceptance and remaining work

I distinguish implemented behavior, executed checks and acceptance within a named
environment. The [evidence index](evidence/README.md) preserves commands, source/model
identities, inputs, original outputs and earlier failures. Native integration,
container qualification and Databricks qualification have separate gates.

## Observed local acceptance

| Criterion | Observed result | Evidence and limit |
|---|---|---|
| Contracts, causal features, delayed labels, sink atomicity, provider guards and monitoring | **333 Python tests passed** on licensed source `185c415` (0 failures, errors or skips); earlier `daaa5b8` source had 294 passes and Ruff passed | [Rehashed original hosted JUnit](evidence/hosted-reference-2026-11-17/README.md) and [earlier receipts](evidence/followup/README.md); named predictor/client doubles remain unit evidence |
| Kafka → Spark → MLflow → persisted outputs | Current reviewed source: **all 21 checks passed** in 200.955 s | [Fresh native result](evidence/followup/native/qualification.json); real single-host services at `daaa5b8` |
| Source reconciliation | 4,380 raw = 2,164 sensors + 2,164 labels + 24 heartbeats + 24 rejects + 4 duplicates | Native receipt; 2,092 features/predictions, 12 alerts, no pending work, zero observed lag in all three partitions |
| Worker/broker recovery and unavailable model | Before/after-commit recovery, retained pending predictions and active-query broker recovery observed | Broker stop-to-restart interval 15.103 s; active worker consumed 303 new recovery records |
| Model deployment and rollback | Candidate and restored baseline scored subsequent real stream input | Native deployment/prediction receipts; numeric activation is separate from the registry alias |
| Stopped-state restoration with continued work | **28/28 checks** in 95.825 s: 303 retained messages and 303 fresh post-restore messages consumed | [Restoration report](evidence/followup/restore/restore-qualification.json); 4,077 raw records, 1,965 predictions, six preserved alerts, immutable history, four worker runs, zero lag and observed owned-process cleanup; cross-host recovery remains untested |
| Fixed-input business invariance | Three 24-hour variants produced identical features, scores, alert business fields and cooldown outcomes | [Benchmark03](evidence/benchmark/README.md); each has 2,317 raw records, 1,141 predictions, 4 alerts and 18 cooldown suppressions |
| Bounded load/resource observation | Four 2-hour/24-hour variants completed in 84.614 s overall | Worker wall times include startup/shutdown; CPU/RSS and latency receipts are scoped observations, without statistical speedup or multi-node capacity claims |
| Model quality gates | Baseline and candidate eligible on the original synthetic holdout | [Model evidence](evidence/model/README.md); 5,166 rows, 24 failures, 17.9375 machine-days |
| Dashboard data and script behavior | Real HTTP HTML/JSON; **13 JavaScript assertions** including HTTP-error recovery, six restored alert rows and 40 prediction rows | [Current check scope](evidence/followup/README.md#dashboard-and-unresolved-environment-gates); historical minimal DOM/fetch doubles only; actual Chrome-rendered proof documented separately |
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

## Public-source security qualification

The default CircleCI workflow additionally defines a small `publication-security` job. It fetches visible branches and tags, rejects shallow history, and captures the exact checkout/ref scope. Existing bounded secret-pattern checks cover current files and reachable Git blobs; installed package licence declarations and Python dependency advisories are reported separately. Original JSON reports remain as artifacts even on failure. This check does not start Kafka, Spark, MLflow, training, Databricks or a paid machine executor.

**Evidence boundary:** a successful CI status is not an independent inspection of raw JSON. The scanner reports but does not inspect oversized/binary files, unreachable Git objects, opaque compressed contents or external services. Compare its skipped list with the separately retained decompressed-evidence review. The maintainer selected **Apache-2.0** for original project source and documentation; third-party dependencies retain separate rights, and the scanner is not legal clearance. Public visibility and licensing do not establish provider or production acceptance.

## Version-bound upstream licence metadata review — 17 November 2026

Four package metadata flags from the licensed-main [source-security inventory](evidence/hosted-reference-2026-11-17/README.md) have a [bounded upstream provenance review](evidence/third-party-licensing-2026-11-17/README.md) using original version-tagged license files and Git blob hashes: `huey==3.5.4` (installed, upstream MIT-style terms), `colorama==0.4.6` (not installed, BSD-style three clauses), `waitress==3.0.2` (not installed, ZPL 2.1), and `pywin32==312` (not installed, several component licence files). No vendored binary/archive extensions were present in the checked P03 source tree. This narrows the four **metadata exceptions**, but does **not** complete notice review for built wheels, container images, JVM artifacts or all transitive licenses. Public source licensing remains Apache-2.0; no provider acceptance is inferred.

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
non-blocking historical/optional CI path. Those older Python/Docker CircleCI
runs alone did not qualify browser rendering, but a **subsequent separate**
real-Chrome presentation run now does. None of these checks qualifies live
Databricks, production accessibility or an unrestricted public release.

## Hosted container execution — source-bound record

CircleCI's `ci/circleci: reference-containers` status is **success** for
[`4bcc55e`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/4bcc55ec470a752bac59dfa45f561dad0261d95e/checks); its [hosted job](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7) uses the pinned
Linux machine image `ubuntu-2404:2026.10.1`, validates Docker Compose and
executes the bounded `scripts/integration.sh` against real services.
The previous [#8 failure](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/8?useNewPipelines=false) is explained by an existing
`evidence/integration` directory and exit 64. Commit `9e6852d` moved host
metadata into `evidence/ci-host`, and `4bcc55e` added a routable ingress
bridge while keeping published host ports bound to loopback.

An intermediate working-branch note **reported** a passing run receipt, a total
of 20 final checks, 4,380 raw rows, 2,092 features/scores, 12 alerts and recovery/
lifecycle assertions. These receipt-level values have **not been independently
reconciled with the original CircleCI artifacts**. In particular, the current
`Store.verify()` method emits **six named checks**, so the reported "20 final
checks" cannot be assigned to `final-verification.json` without inspecting it.

**Integrity defect found:** the four strings published as SHA-256 contain 65,
65, 65 and 66 hex characters respectively, whereas a SHA-256 hex digest always
contains 64. They are **invalid as reported**, not just unverified. They are
preserved strictly as historical text in [the CircleCI manifest](evidence/circleci/manifest.json),
with no trusted digest baseline. Do not truncate or guess them. The
[offline intake procedure](evidence/circleci/README.md) and
[locally fixture-tested verifier](../scripts/verify_circleci_artifacts.py) can
recompute actual hashes from an original downloaded artifact set. The checker
returns `observed_unpinned`, not `verified`, until a separately reviewed valid
SHA-256 baseline is established.

**CircleCI artifact retrieval boundary (16 November):** the public v2 API lists
artifacts for the successful `reference-containers` job #13, but attempted raw
unauthenticated download of the four receipts and host commit returned HTTP 404.
CircleCI requires authorization for raw artifacts; **this does not demonstrate
that their bytes are lost or verified**. A [bounded authenticated downloader](../scripts/download_circleci_artifacts.py)
and its [intake procedure](evidence/circleci/README.md) now support local
collection using `CIRCLE_TOKEN` from a secure environment. Neither has been run
with a connected authorized CircleCI token in this session.

| CircleCI artifact | Historical reported string — INVALID SHA-256 | Length |
|---|---|---|
| `integration/run-receipt.json` | `78b45855d20b1ab5c6784f59eff21dd8e42f2c0dedc345f8383cfc19312d148a9` | 65 |
| `integration/final-verification.json` | `5026ba8908142f6e21ca83a1ff2a03be6be63d9df3bb6f348a02723463d4383dd` | 65 |
| `integration/lifecycle-assertions.json` | `c9d805ba5503796db98a688e9b593de147744d2019455fe11b8d659811eb3c0de` | 65 |
| `integration/recovery-assertions.json` | `66589288bdba4bb79b6068baab056fa822fa1545df851779f407ce62e7ec42512e` | 66 |

This hosted Docker reference remains single-host synthetic validation, not
Databricks acceptance, distributed scalability or production readiness.

## Independently inspected original Chrome evidence — 16 November 2026

The hosted [GitHub Actions run 43921124746](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746)
executed **real Chrome 157.0.8103.50** and uploaded original
[artifact 14253962621](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746/artifacts/14253962621).
All **19 ZIP entries** were downloaded and inventoried. Original PNG and JSON SHA-256
digests were recomputed for each scenario, and the three images were visually
inspected. The Git blob of the tested `dashboard.py` equals the one on
`main` (`bb05e07f88b8aaf8d1ea6918979ddd82b6ff6f48`).
A fresh **17 November** isolated recheck confirmed all three PNG SHA-256, three
JSON SHA-256, summary hash and screenshot dimensions from the original
218,970-byte ZIP. Its ZIP digest is recorded in the
[manifest](evidence/browser-2026-11-16/manifest.json).
The run's PR head was `2148c9f`; `source-commit.txt` records GitHub's
synthetic merge checkout `d7ee564`. GitHub metadata confirms the commits share
**exact Git tree `4641b61794ea5d3dd4af01fc7459ee17a6431200`**, so there is
no source content discrepancy. This is an assisted original-byte recheck, not
independent human certification. [Proof and limits](evidence/browser-2026-11-16/README.md).

| Scenario | Original image SHA-256 | Result |
|---|---|---|
| Healthy desktop, 1440×1000 | `1787d7001d6f847b7ea1f06086db443f5f23ec3b06f94187848b4261353d31eb` | PASS |
| Healthy responsive, 600×850 | `94d8f215a069630fcecec34e862b9afcbc878b1ef10ef76698888cd6399cc1c2` | PASS |
| HTTP 503, desktop | `3041e000638b5974e80f6d40f9f0e4c02c9449213100813a21325ce30175d480` | PASS |

The first narrow visual capture exposed a real header/status collision; CSS and
a DOM-geometry assertion corrected this, and the final captures report
`horizontal_overflow=false`, `header_overlap=false`,
`external_resources=0`. Historical blocked/failing executions are preserved.
**Scope:** real browser presenting the exact application HTML, but with **synthetic
HTTP snapshot data**. No live Kafka/Spark/MLflow, Databricks, paid provider,
accessibility conformance or independent human-operator acceptance is inferred.

## Original hosted Docker and checks reinspection — 17 November 2026

A separate [GitHub Actions real-integration run](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43983190446) completed on Apache-2.0 source `185c415`. This is **additional evidence**, not a replacement for CircleCI as the selected CI. The actual **57-file integration ZIP** and **5-file checks ZIP** were downloaded through the authorized repository connector; their original ZIP/member SHA-256 values, source commit and runtime inventory copies were independently recomputed and [recorded](evidence/hosted-reference-2026-11-17/README.md).

The run has **333 successful tests**, and Docker service receipts show **six true persisted-data invariants**, a final **2,513 raw inputs, 1,225 predictions and 7 alerts**, no pending records, zero observed Kafka lag, real model-version rollback, and cleanup exit code zero. Initial `verification.json` contains **2,317 raw records**, while the distinct `final-verification.json` contains **2,513** after **196 additional rollback inputs**. These stage receipts are expected to differ and reconcile. The independent check reports 123 dependency entries without published findings, zero secret patterns in 351 worktree files / 462 reachable blobs, and four licence-metadata review flags (three not installed on the runner). These limits are detailed in the manifest.

The historical **CircleCI job #13 original bytes remain unverified** because raw artifact access requires an authorized token. A successful newer GitHub Actions Docker run and its rehashed original archives do **not** retroactively verify those older CircleCI bytes. Databricks remains a separate provider gate.

## Open qualification and release gates

| Gate | Current state | Evidence needed to close it |
|---|---|---|
| Original Docker evidence | **New source-bound recheck achieved** at `185c415` with exact original [GitHub-hosted Docker ZIP and six business checks](evidence/hosted-reference-2026-11-17/README.md). The older CircleCI job #13 was successful but its original archive bytes remain unaudited. | If CircleCI job #13 provenance is separately required, recover its original bytes via the [token-from-environment downloader](evidence/circleci/README.md); do not substitute the newer GH archive as proof of those original hashes |
| Databricks | **Open**; no authorized workspace run | Existing authorized compute/storage, Kafka access, provider run IDs, Delta versions and recovery evidence |
| Browser presentation | **Chrome 157 passed and its 3 original images + JSON receipts were independently inspected** (1440px desktop, 600px responsive, HTTP 503); no horizontal overflow, header badge overlap or external resources after the correction. The blocked local Chrome attempt is historical. | [Artifact / exact SHA manifest](evidence/browser-2026-11-16/README.md); deliberately synthetic snapshot and source HTML, not Kafka-backed UI or independent operator/accessibility acceptance |
| Release publication | Planned tag `v0.1.0-rc.1` maps to Python package `0.1.0rc1`. All 3 CircleCI jobs succeeded on merged `a6b4d97`, including **339 tests** and a real wheel build with SPDX/NOTICE/CLI checks. Complete wheel module-byte/PEP 427 `RECORD` verification is added for the next RC qualification; its own final-head CI must pass. No tag, PyPI upload or GitHub release is published. | [RC version/wheel PR #18](https://github.com/mehdisafer/industrial-streaming-mlops/pull/18) and [prepared notes](release-notes.md). Publication remains a separate owner action bound to the final reviewed SHA. |
| License and visibility | **Public and GitHub-recognized Apache-2.0** as re-read 17 Nov at `185c415`: root [LICENSE](../LICENSE), [NOTICE](../NOTICE), PEP 639 package SPDX and corresponding tests are merged | GitHub REST reported `license.spdx_id=Apache-2.0`; this confirms the original-source grant but does **not** license third parties or qualify live Databricks, provider execution or distribution compliance |
| Repository settings | GitHub readback 17 Nov: description `null`, topics `[]`, `main.protected=false`, rulesets `[]`; classic protection-details endpoint inaccessible to this connector | Owner must apply About metadata and suitable PR/CI protections in GitHub's interface, then re-read actual saved rules and required contexts; no configuration change is claimed |

The [historical check receipts](evidence/checks/README.md) preserve GitHub Actions
jobs that failed before runner allocation (`runner_id=0`, no executed steps).
The maintainer reports an exhausted GitHub Actions quota. The annotation is
inaccessible, so quota exhaustion is not independently verified from those
jobs. This historical failure is not an application test failure or a P03 gate.
CircleCI remains the acceptance CI. Public visibility is now observed; no plan
upgrade, quota restoration or GitHub Actions rerun is required or implied.
Recent GitHub Actions runs did execute their Python/security checks on PR merge
revisions, which does not replace any scoped CircleCI Docker/provider gate.

Historical GitHub Actions CI scope selection was locally exercised with real
temporary Git history: documentation-only updates skip heavy integration,
source changes select it, and manual dispatch selects the full path.
Job-level concurrency behavior is implemented and statically checked but
**that historical scheduling check was not exercised at the time**. Later GitHub
Actions runs did execute on PR revisions; this does not establish Docker acceptance
or change CircleCI's role as the designated acceptance CI. No paid runner or cloud resource
was provisioned or requested.

## Prioritized follow-up

1. Obtain the original CircleCI Docker artifacts from source `4bcc55e`;
   fix the **invalid 65/65/65/66-character historical hash claims** using the
   [offline intake verifier](evidence/circleci/README.md). Record actual bytes,
   six named `Store.verify()` checks, model lifecycle/recovery, image identities
   and scoped cleanup. Do not infer 20 checks from a six-check JSON report.
   GitHub Actions runner allocation/quota is **out of P03 acceptance scope**.
2. Run the [Databricks qualification](databricks.md) on explicitly authorized existing
   resources, including library/access checks, business-output parity, recovery and
   available consumption data. Local Spark is not provider qualification.
3. Retain the independently rehashed original hosted synthetic dashboard-browser
   screenshots at their exact source SHA, and qualify live-data and operator/
   accessibility behavior separately. The source is public and Apache-2.0 is
   selected; third-party notice review, About metadata, branch protection and
   release-tag publication remain separate decisions.
4. Before unattended local operation, define database/evidence retention, archival,
   disk limits, service supervision and recovery objectives. The current finite demo
   profile and Kafka retention setting do not implement complete local retention.
5. Before larger deployment, implement distributed durable feature state, multi-node
   recovery tests and authenticated service exposure. SQLite remains a single-writer
   boundary, and adding local Spark threads does not remove it.
6. Evaluate representative external telemetry and unseen sites, with calibrated scores,
   uncertainty estimates and maintenance costs, before claiming field predictive value.
