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

## Public-source security qualification

The default CircleCI workflow additionally defines a small `publication-security` job. It fetches visible branches and tags, rejects shallow history, and captures the exact checkout/ref scope. Existing bounded secret-pattern checks cover current files and reachable Git blobs; installed package licence declarations and Python dependency advisories are reported separately. Original JSON reports remain as artifacts even on failure. This check does not start Kafka, Spark, MLflow, training, Databricks or a paid machine executor.

**Evidence boundary:** a successful CI status is not an independent inspection of raw JSON. The scanner reports but does not inspect oversized/binary files, unreachable Git objects, opaque compressed contents or external services. Compare its skipped list with the separately retained decompressed-evidence review. No project licence has been selected; public visibility alone does not grant an open-source licence. The public status must not be presented as full provider or production acceptance.

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
non-blocking historical/optional CI path. Neither CircleCI result qualifies
Databricks, rendered browser behavior or a public release.

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

| CircleCI artifact | Historical reported string — INVALID SHA-256 | Length |
|---|---|---|
| `integration/run-receipt.json` | `78b45855d20b1ab5c6784f59eff21dd8e42f2c0dedc345f8383cfc19312d148a9` | 65 |
| `integration/final-verification.json` | `5026ba8908142f6e21ca83a1ff2a03be6be63d9df3bb6f348a02723463d4383dd` | 65 |
| `integration/lifecycle-assertions.json` | `c9d805ba5503796db98a688e9b593de147744d2019455fe11b8d659811eb3c0de` | 65 |
| `integration/recovery-assertions.json` | `66589288bdba4bb79b6068baab056fa822fa1545df851779f407ce62e7ec42512e` | 66 |

This hosted Docker reference remains single-host synthetic validation, not
Databricks acceptance, distributed scalability or production readiness.

## Open qualification and release gates

| Gate | Current state | Evidence needed to close it |
|---|---|---|
| CircleCI container reference | **Job succeeded on source `4bcc55e`**, but the four reported digest strings are malformed and no original archive was independently rehashed | Obtain the original [job artifacts](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7), run the [offline verification procedure](evidence/circleci/README.md), independently review the computed 64-character digests and pin a correct baseline |
| Databricks | **Open**; no authorized workspace run | Existing authorized compute/storage, Kafka access, provider run IDs, Delta versions and recovery evidence |
| Browser presentation | **CircleCI `dashboard-browser` succeeded on `main` `f83bec3`**, exercising fixture-backed Chrome desktop/narrow/HTTP-outage views; the 15 November native Chrome socket error remains a separate historical observation | [Hosted job](https://app.circleci.com/workflow/3db01d8e-aad5-403c-ab43-d3a3bc6b8629/job/60e9d9c5-f51b-49fd-aed3-f4c474095c48); original screenshots and live Kafka-driven UI still require independent review |
| Release publication | `v0.1.0-rc.1` notes prepared; no tag or GitHub release published | [PR #2](https://github.com/mehdisafer/industrial-streaming-mlops/pull/2) and [follow-up PR #3](https://github.com/mehdisafer/industrial-streaming-mlops/pull/3) record source review; tag/release publication requires its own verified action |
| License and visibility | **Public** according to GitHub repository API (16 Nov 2026); no approved project licence | Public source is not an open-source licence grant; licence choice and anonymous content readback remain open |
| Repository settings | Description is null and topics empty (16 Nov). Current rulesets API returns `[]`; branch-protection read remains 403 for this integration | Maintainer applies About metadata and checks available `main` protections; empty rulesets do not establish whether classic protection is enabled |

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
3. Retain the hosted synthetic dashboard-browser result at its tested SHA, inspect
   original screenshots, then qualify live-data browser behavior separately. The source
   is public, but licence, About metadata, branch protection, anonymous readback and
   release-tag decisions remain for independent maintainer review.
4. Before unattended local operation, define database/evidence retention, archival,
   disk limits, service supervision and recovery objectives. The current finite demo
   profile and Kafka retention setting do not implement complete local retention.
5. Before larger deployment, implement distributed durable feature state, multi-node
   recovery tests and authenticated service exposure. SQLite remains a single-writer
   boundary, and adding local Spark threads does not remove it.
6. Evaluate representative external telemetry and unseen sites, with calibrated scores,
   uncertainty estimates and maintenance costs, before claiming field predictive value.
