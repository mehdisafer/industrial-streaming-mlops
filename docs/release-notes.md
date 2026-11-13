# v0.1.0 release candidate

## Intended outcome

An executable synthetic maintenance workflow with real Kafka ingestion, Spark
micro-batches, causal feature state, MLflow numerical model versions, alert evidence,
operator monitoring and explicit recovery.

## Included changes

- Strict versioned contracts and deterministic sensor/failure/label scenarios.
- Shared point-in-time feature computation and semantic compatibility signatures.
- Temporal train/validation/test protocol with a fixed maintenance alert budget.
- Persistent HTTP MLflow tracking, reviewed model serialization, promotion and rollback.
- Atomic local source/feature/prediction/alert sink, checkpoint binding and replay controls.
- Read-only maintenance dashboard, source dispositions and observed-alert monitoring.
- Bounded native/container qualification, failure injection and restoration procedures.
- Existing-compute Databricks bundle, Delta adapter and evidence collection.
- Frozen Python/JVM dependencies, CI and component/security inventory.

## Evidence requirement

The reviewed pull request and `docs/evidence/` identify the exact execution receipts.
Native qualification, container qualification and provider qualification are separate.
No production reliability, distributed capacity or vendor qualification is inferred
from a unit test, local run or deployment template.

## Remaining release decisions

- The owner must explicitly select the repository license. No LICENSE file has been
  invented or copied from another project.
- Public visibility has not been authorized. The repository remains private.
- Databricks requires an authorized workspace, existing compute and storage,
  reachable Kafka and trusted model artifacts, with a bounded run budget.
- Repository administration capabilities are not exposed by the current GitHub
  connector. Description, topics and branch protection are prepared below for
  an authorized administration step; they are not reported as configured.

Suggested description:

> Traceable industrial maintenance alerts with Kafka, Spark, MLflow and a separately qualified Databricks deployment.

Suggested topics:

`data-engineering`, `mlops`, `kafka`, `spark`, `structured-streaming`, `mlflow`,
`databricks`, `predictive-maintenance`, `data-quality`, `python`

Suggested main-branch rule: require the meaningful CI checks and a pull request;
disable force pushes and deletion; do not invent an independent approval or purchase
a plan to obtain unavailable private-repository protections.

## Migration and rollback

The initial persistence schema is version 1. Configuration identity is bound to the
store and checkpoint. Changed feature semantics require a new dataset/feature/model
qualification and an explicit store/checkpoint migration. Never delete a checkpoint
to make an incompatible change appear successful.

Model rollback selects the previous compatible activated numerical version. Existing
predictions remain immutable and attributable to the model that actually produced them.
See the [operations runbook](operations.md) for stopped-state backup and relocation.
