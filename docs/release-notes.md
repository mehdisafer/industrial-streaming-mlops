# Prepared notes for v0.1.0-rc.1

This is a **public source reference** for an executable synthetic maintenance workflow; the release candidate has not been tagged or published as a GitHub release.
The notes are prepared for release review; no `v0.1.0-rc.1` tag or GitHub release has
been published. [PR #2](https://github.com/mehdisafer/industrial-streaming-mlops/pull/2),
[follow-up PR #3](https://github.com/mehdisafer/industrial-streaming-mlops/pull/3)
and the [acceptance register](acceptance.md) track review and outstanding gates.

## Intended outcome and changes

I built this reference so that a maintenance warning can be traced from Kafka
coordinates through a causal sensor window, numerical MLflow model version and
eventual delayed label. It includes:

- Strict versioned sensor/label/heartbeat contracts, fixed units and deterministic
  degradation/failure scenarios with duplicate, late and poison inputs.
- Shared point-in-time features, warm-up behavior and semantic compatibility
  signatures; labels and simulator truth never enter the feature matrix.
- Temporal train/validation/test evaluation with a fixed false-alert budget,
  validation-selected thresholds and an explicit explainable baseline.
- Persistent HTTP MLflow tracking and registry, reviewed artifact serialization,
  numerical activation, separate alias promotion and compatible local rollback.
- Atomic SQLite source/feature/prediction/alert persistence, exact original payload
  bytes, checkpoint binding and replay controls.
- A read-only dashboard and monitoring based on persisted alerts and available
  labels; drift does not automatically promote a model.
- Bounded native/container qualification procedures, fault injection, stopped-state
  backup/restore, and an existing-compute Databricks/Delta qualification path.
- Locked Python/JVM dependencies, scoped CI selection, isolated container smoke
  safeguards, component inventory and original evidence with source hashes.

## Executed verification

| Evidence | Observed result |
|---|---|
| [Current application checks](evidence/followup/README.md) | 294 tests passed in 19.29 s, zero failures/errors/skips; Ruff passed; 122 third-party packages audited with zero known findings, one local project explicitly skipped |
| [Hosted CircleCI Python CI](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/5?useNewPipelines=false) | **Success** on `579cbc5`, repeated on descendant `ce7674c` in [pipeline #7](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/7?useNewPipelines=false); merged in [PR #4](https://github.com/mehdisafer/industrial-streaming-mlops/pull/4). Unit lane only; CI JUnit assertion counts not read |
| [CircleCI Docker job](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7) | **Success** on `4bcc55e`, distinct from the earlier exit-64 failure on `ce7674c`. Docker runtime provenance and business receipts are externally retained in CircleCI; reported contents/hashes not independently rechecked here |
| [Fresh native execution](evidence/followup/native/qualification.json) | All 21 checks passed in 200.955 s at `daaa5b8`; 4,380 raw records, 2,092 predictions, 12 alerts and zero observed source lag across three partitions |
| [Recovery and rollback](evidence/followup/native/qualification.json) | Before/after-commit recovery, unavailable-model replay, active broker interruption, candidate activation, rollback and restored caught-up checkpoint with no duplicate effects |
| [Restored backlog and fresh input](evidence/followup/restore/restore-qualification.json) | 28/28 checks in 95.825 s; 303 retained records then 303 fresh records; immutable history, 1,965 final predictions, four worker runs and observed owned-process cleanup |
| [Model holdout](evidence/model/README.md) | On 5,166 labeled rows / 24 failures, candidate useful recall 100%, alert precision 96%, one false alert (0.055749 per machine-day); baseline useful recall 87.5%, precision 42%, 29 false alerts |
| [Streaming benchmark03](evidence/benchmark/README.md) | Four 2-hour/24-hour variants completed in 84.614 s; the three identical 24-hour inputs produced exact feature/score/alert/cooldown parity, each with 1,141 predictions, four alerts and 18 cooldown suppressions |
| [Dashboard checks](evidence/followup/README.md#dashboard-and-unresolved-environment-gates) | Real HTTP HTML/JSON and 13 assertions on actual JavaScript with explicit DOM/fetch doubles, including successful recovery after an HTTP error; six restored alert rows and 40 prediction rows |

The benchmark's worker throughput includes Python/JVM startup and shutdown. The
24-hour variants measured 143.18, 122.23 and 124.27 raw records per worker wall second
for one partition/one thread, three partitions/one thread and three partitions/two
threads respectively. These single-host, single-run observations do not establish
statistical speedup, distributed capacity or production service levels.

Model results describe generated data and simulated lead from feature availability;
they do not establish industrial generalization or wall-clock advance warning. The
original native01 training result was reused unchanged for the original native08
qualification. The follow-up restoration trained the same fixed protocol, and its
stopped model state was reused for the fresh fault/rollback run after source checks.
Earlier failed/blocked attempts and the superseded native07 lag observation remain
in [the attempt history](evidence/attempts/README.md). The [evidence index](evidence/README.md)
links exact source identities and original receipts rather than substituting a later
commit for code that was actually executed.

## Open gates and publication decisions

- **Hosted Docker reference passed** in CircleCI [`reference-containers`](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7) on source `4bcc55e`; the older [#8](https://app.circleci.com/pipelines/circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu/8?useNewPipelines=false) failed before integration because its wrapper pre-created `evidence/integration`. Later `9e6852d` corrected the evidence directory and `4bcc55e` corrected loopback service ingress. **The four earlier strings described as SHA-256 are malformed (65, 65, 65 and 66 characters) and not usable as integrity proof.** Actual original receipt bytes remain required for the [offline artifact procedure](evidence/circleci/README.md). The public CircleCI API lists job #13 artifacts, but raw URLs returned HTTP 404 without authorized access. The new [bounded authenticated downloader](../scripts/download_circleci_artifacts.py) is prepared for an operator-provided `CIRCLE_TOKEN` environment variable; it was **not executed with credentials** in this delivery. Do not claim independent artifact integrity.
- **CircleCI is the required hosted CI.** Its Python checks and separate Docker
  reference job succeeded on the recorded source commits. The maintainer reports
  an exhausted GitHub Actions quota; the inaccessible runner annotation does not
  independently prove that explanation. GitHub Actions failures before runner
  allocation remain historical evidence, **not an open release prerequisite**.
  No quota purchase, Actions rerun or billing change is required.
- **Databricks remains open.** It needs an authorized workspace, existing compute
  and storage, reachable Kafka and trusted artifacts, with a bounded run budget.
- **Bounded browser presentation succeeded in CircleCI.** The [dashboard-browser job](https://app.circleci.com/workflow/3db01d8e-aad5-403c-ab43-d3a3bc6b8629/job/60e9d9c5-f51b-49fd-aed3-f4c474095c48) reported success on exact main commit `f83bec3` with synthetic Chrome desktop/narrow/outage views. The earlier local Chromium Unix-socket failure remains historical. Original screenshots have not been independently reviewed here, and live-stream UI acceptance is distinct.
- GitHub reports the source repository **public as of 16 November 2026**. The owner has not selected a repository licence; public visibility does not confer an open-source reuse grant. Third-party components retain their own licences. Anonymous GitHub repository/README readback was independently observed on 16 November; the project licence, available protections and GitHub release remain separate acceptance gates.
- Repository readbacks show description `null` and topics `[]`. The available
  integration exposes no supported metadata or release mutation tools, so the
  proposed settings and these release notes remain prepared material.
- Earlier private-repository ruleset queries returned plan/visibility errors. A fresh
  16 November GitHub rulesets query returns an empty list; the classic main-branch
  protection endpoint still returns HTTP 403 for this integration. These observations
  do not prove whether classic protections are active. Recent hosted GitHub Actions
  runs have executed successfully after earlier pre-runner failures. No plan upgrade,
  additional visibility change, protection modification or release publication is implied.

The [original check/API receipts](evidence/checks/README.md) record those observations.
The PR records the source review and verified merge state. Tag/release publication
requires its own verified action and is not implied by merging the source.

Suggested description:

> Traceable industrial maintenance alerts with Kafka, Spark, MLflow and a separate Databricks qualification path.

Suggested topics:

`data-engineering`, `mlops`, `kafka`, `spark`, `structured-streaming`, `mlflow`,
`databricks`, `predictive-maintenance`, `data-quality`, `python`

Suggested main-branch rule: require the meaningful checks and a pull request;
disable force pushes and deletion. Any integration evidence requirement should apply
to source changes while allowing documented documentation-only skips. No independent
approval or paid protection plan is assumed.

The workflow now gates heavy integration by changed source/configuration paths.
Documentation-only pushes do not cancel a running heavy job: concurrency applies
only to the selected integration job, with running-job cancellation disabled and
one pending job per PR/ref group. Hosted scheduler behavior still needs execution.

## Migration, rollback and operating limits

The initial persistence schema is version 1. Configuration identity is bound to the
store and checkpoint. Changed feature semantics require a new feature/model
qualification and an explicit store/checkpoint migration. General in-place migration
is not implemented; preserve the original state and retained source data.

`rollback` activates the previous compatible local numerical deployment. It leaves
registry aliases separate and preserves existing predictions with their original
model provenance. The [operations runbook](operations.md) describes stopped-state
backup and the controlled relocation of a verified copied checkpoint. The original
restore resumed an already caught-up checkpoint. The follow-up separately
qualified retained backlog and fresh input after restoration while preserving old
labels, observations, predictions and deployment history. Cross-host restoration
remains additional qualification work.

The provider follow-up adds immutable alert decisions, explicit checkpoint ownership,
loaded-library checks, native Unity Catalog registry constraints and a separate hard
driver row cap. Use the fresh namespace/table-prefix/checkpoint procedure in the
[provider runbook](databricks.md); a general in-place provider migration is not claimed.

This release candidate remains a bounded single-host reference. Complete local
database/evidence retention, supervision, authenticated exposure, distributed feature
state, multi-node recovery and representative external model evaluation are
[prioritized follow-up](acceptance.md), not delivered production capabilities.
