# Databricks qualification

**Status: prepared; provider execution and acceptance remain open.** No workspace,
compute, Kafka credential or provider execution receipt is assumed. Local Spark
evidence is kept separate.

## What the bundle actually runs

The bundle defines one bounded job on an explicitly supplied **existing** cluster.
It creates no cluster, SQL warehouse, serving endpoint, catalog, schema, volume or
secret scope. There is no schedule, retries are disabled, concurrency is one, the
task timeout is 20 minutes and the overall job timeout is 25 minutes.

The job reads a real Kafka source in `availableNow` mode and writes these Delta
tables under a unique prefix in an existing authorized catalog/schema:

| Table suffix | Content |
| --- | --- |
| `bronze` | Original Kafka key/value bytes, topic, partition, offset and broker timestamp |
| `dispositions` | Exactly one accepted, duplicate or rejected disposition per source coordinate |
| `accepted` | Strictly validated, deduplicated events with source-time metadata |
| `labels` | Available-label contract records, separate from sensor inputs |
| `features` | The shared causal feature vector and its source event identities |
| `predictions` | Real registered-model inference, immutable score and alert decision, exact model version and evidence payload |
| `alerts` | Threshold and cooldown policy outcomes, linked to predictions |
| `batches` | Completion records written after the other outputs |
| `deployment` | Immutable source/config/model identity for the table/checkpoint namespace |

SQLite is not used on a cloud mount. Each table uses an idempotent Delta `MERGE`.
The tables do **not** form one atomic transaction. A failed batch may leave
partial tables visible; its completion record is absent and replay finishes the
remaining writes. A provider failure/restart exercise is still required before
claiming that recovery behaviour is verified on Databricks.

Label relationship checks match the shared replay contract. An orphan label, a
label whose dataset/site/machine/time disagrees with its sensor, or a second label
for the same anchor is explicitly rejected. A rejected label does not establish
the Kafka partition assignment of the machine it claims to describe.

New predictions are ordered by feature availability, event time and feature ID,
matching the local cooldown order. Each prediction persists its `alert_emitted`
decision before the separate alert-table write. Already committed predictions are
not rescored or assigned a new decision. If a later write fails, a retry reconstructs
missing alert rows from those decisions and checks the persisted alert rows before
writing the completion record. Prior emissions seed cooldown for new features,
including a feature whose supplied availability timestamp is earlier than an already
committed emission. No historical alert is silently rewritten by a later replay.

Performance evaluation joins available labels to actual persisted alert rows. It
does not reconstruct cooldown after filtering out unavailable labels. Reports
distinguish emitted alerts, evaluable alerts and `pending_alert_labels`; a missing
outcome does not turn a suppressed prediction into a reported alert. Undefined
precision/rates remain null when the required evidence is absent.

To preserve feature parity, this first provider adapter replays the bounded raw
dataset on the driver using `replay_with_report`. The default limit is 50,000 raw
records, the allowed maximum is 200,000, total replay payload is capped at 64 MiB,
and the micro-batch default is 1,000.
Exceeding the bound stops the job after preserving raw input; it never silently
drops records. This design is deliberately labelled a qualification adapter. A
distributed Delta state/feature implementation and its partition-invariance/load
proofs remain separate backlog work.

## Required authorized inputs

Use isolated qualification compute rather than a shared business cluster because
the job installs pinned model libraries. The reference target is DBR 15.4 LTS;
the task-library overlay still needs actual compatibility verification. PySpark
is supplied by DBR and is never installed from the project wheel.
The task also requires the native `dbutils` interface from compatible classic
compute. It explicitly rejects the pinned SDK's `RemoteDbUtils` fallback, which
can be selected when the native namespace is unavailable, including some Spark
Connect contexts. The remote implementation has different directory metadata and
path support. An existing-cluster ID alone does not establish this compatibility.

The published DBR 15.4 baseline includes NumPy 1.23.5, SciPy 1.11.1 and PyArrow
14.0.1. The model overlay uses NumPy 2.3.5, so retaining those older numerical
binaries is not a compatible assumption: the SciPy 1.11 support table excludes
NumPy 2, and Arrow's NumPy 2 compatibility work was completed in version 16.
The task therefore also pins SciPy, PyArrow and the Databricks SDK from the reviewed
Python 3.11 lock. These are explicit task libraries, not an installation of PySpark
or a change to the existing cluster definition.

| Used task dependency | Pin |
| --- | --- |
| MLflow | 3.17.2 |
| NumPy | 2.3.5 |
| pandas | 2.2.3 |
| scikit-learn | 1.8.0 |
| skops | 0.16.0 |
| SciPy | 1.17.1 |
| PyArrow | 25.0.1 |
| Databricks SDK | 0.166.0 |

Before model access or table writes, the job checks the installed distribution
versions and the versions of the actually loaded modules, then performs small
NumPy/SciPy, Arrow, pandas and scikit-learn operations. Its result is recorded in
`runtime_import_preflight`.
It rejects a stale preloaded module even when the distribution metadata already
reports the new version. This check can catch missing pins and common binary
interface failures; it does not validate every preinstalled vendor package,
Spark/Delta integration or provider permissions. Disabling Spark Arrow conversion
alone would not validate Python's PyArrow import. The actual overlay still
requires execution on the selected compute.

The operator must supply the workspace identity, existing compute ID, catalog,
schema, writable volume/output location, durable checkpoint location, Kafka
endpoint/topic, existing Kafka secret scope/key, and a trusted numerical model
version. The Kafka adapter currently supports **SASL_SSL + PLAIN**. The secret is
the complete JAAS configuration; it is fetched inside the run and never written
to a bundle variable, repository file or evidence report. Other mechanisms need
a reviewed adapter change rather than an invented successful connection.

The model must already be available from the selected registry, carry the
project's publisher, ordered feature names, feature version, held-out gates,
threshold, evaluation identity, matching alert-budget policy and artifact digest,
and be explicitly qualified. The serializer must be skops with the exact
`model.skops` filename and the reviewed trusted-type list. Bundled loader code
paths and expanded artifact-provided trust lists are rejected. The registered
version must resolve through an MLflow 3 logged-model identity with the same
source run. This provider-side artifact API/layout still requires a real test;
an unsupported imported model layout fails instead of bypassing the policy.
For Unity Catalog use `catalog.schema.model`; for an existing workspace registry
select its URI explicitly. A local `http://localhost:5000` model is not accessible
from Databricks. Moving it requires an authorized model import/registration with
its original provenance; the bundle does not expose the local tracking server.

## Checkpoint ownership preflight

Use a dedicated checkpoint child directory whose parent already exists and is
listable by the selected compute. Supported locations are below an authorized
`/Volumes/catalog/schema/volume/` directory or an `abfss://`, `s3://` or `gs://`
location. A volume or bucket root, traversal components, query parameters and
fragments are rejected. The guard rejects a parent or checkpoint listing with more
than 10,000 returned entries; choose a small, isolated parent for the qualification.

After selecting the native interface, before any Delta or streaming write, the
job checks the existing Delta deployment identity, then uses the documented
`dbutils.fs.ls`, `head`, `mkdirs` and `put`
operations to bind the checkpoint. A new deployment requires an empty checkpoint
or an absent child under that existing parent. The job writes a bounded
`_industrial_deployment.json` marker without overwrite and reads it back. The
marker binds the source hashes, code commit, configuration, model, Kafka topic and
endpoint digest, checkpoint path, catalog, schema and table prefix.

An existing deployment must have the same marker. A populated unbound checkpoint,
a changed binding, a missing checkpoint for an existing namespace, or a populated
checkpoint whose namespace is missing stops initialization. A matching marker
alone can resume initialization if the process stopped before creating Delta
tables. Listing and marker read failures propagate; a permission or transport
error is never interpreted as an absent or empty path. The job does not adopt,
delete or overwrite an old checkpoint to make a run succeed.

This guard assumes one writer owns the namespace and checkpoint. The job's own
concurrency limit does not prevent a separate job or an operator from writing the
same paths. The marker is not an object-store lock, and no atomic transaction spans
the filesystem and Delta. If initialization fails after creating a marker, preserve
the path and reconcile its state before another run. Actual cloud filesystem
semantics, Spark's use of the bound checkpoint and failure/restart recovery remain
provider acceptance work.

## Read-only preflight

Install a modern [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
from the vendor distribution. Bundles require at least 0.218.0; record the exact
CLI version actually used. The current bundle schema is sourced from the vendor
documentation, but no CLI/workspace validation is claimed before it runs.
Authenticate with an authorized CLI profile using the vendor's supported OAuth
workflow. For automated CI, use an authorized service principal and external
credentials; never commit a token.

```bash
python databricks/qualify.py --profile qualification preflight \
  --cluster-id "$EXISTING_CLUSTER_ID" \
  --output evidence/databricks/preflight.json
```

The preflight reads compute metadata, the authenticated identity ID and ACLs.
ACL presence alone does not determine effective group permissions. Check the
actual ability to attach/run, install task libraries, read the model and secret,
connect to Kafka and write the selected tables/volume. A stopped cluster is
reported; the script does not start it or authorize spend.
The receipt records whether the reported runtime matches the 15.4 reference. This
metadata read does not execute the job's import preflight on the remote compute.

## Reproducible deployment and bounded run

Supply the variables as local environment variables (`BUNDLE_VAR_<name>`) or an
ignored local bundle override. They are identifiers and paths, not secret values.
Required variable names are listed in `databricks.yml`. Set `code_sha` to the
reviewed Git commit and upload a clean checkout of that commit. Use a distinct
`table_prefix`, `output_root` subdirectory and `checkpoint_root` for each intended
source/config/model deployment. Keep each machine on one Kafka partition.
Prepare the checkpoint's existing listable parent, leave its dedicated child
empty, and reserve both the checkpoint and output namespace for this one job.

After explicit authorization for the concrete workspace, compute and bounded
run, the operator executes:

```bash
databricks bundle validate --target qualification --profile qualification
databricks bundle deploy --target qualification --profile qualification
databricks bundle run qualify_stream --target qualification --profile qualification
```

Deployment creates/updates this job and uploads source; running it consumes the
selected compute and installs task libraries. These are separate steps. The
project does not invoke them implicitly. The bundle's existing-cluster reference
is intentional; Terraform adds no useful supported resource in this scope.

If a launch request has an uncertain result, inspect the job runs before retrying.
For a REST-driven launcher, [Jobs `run-now`](https://docs.databricks.com/api/jobs/v2/run-now)
supports an `idempotency_token` of at most 64 characters. Reuse the original token
when reconciling that launch; do not create an unrelated replacement run.

## Evidence and acceptance

The job requires a real Databricks runtime and a resolved provider run ID. A
completed bounded run writes `run-<run_id>.json` below the supplied output volume.
It records code/config/model identity, runtime and package versions, query IDs,
source progress, source-file hashes, table counts and latest Delta history versions, reconciliation,
label coverage, prediction/alert counts and evaluation when labels are available.
The `checkpoint_binding` receipt records marker verification and its manifest
digest; `runtime_import_preflight` records installed and loaded package versions
and the numerical smoke results. `native_dbutils_preflight` records the selected
utility implementation; it does not itself execute filesystem operations.
The per-batch report distinguishes `new_predictions` from total committed predictions
and retains pending outcome counts instead of treating them as evaluated alerts.
Zero predictions fail the run. Set `expected_records` before execution to make
the source count an acceptance assertion; its default zero leaves that assertion
unspecified.

```bash
python databricks/qualify.py --profile qualification collect \
  --run-id "$DATABRICKS_RUN_ID" \
  --output evidence/databricks/run-receipt.json
```

Retrieve the job-generated evidence from the authorized volume and reconcile it
with the Jobs API receipt. Preserve the original logs privately; redact access
identifiers and credentials before selecting evidence for a public release.
The Git SHA is a deployment parameter; reconcile it with the recorded source-file
hashes before asserting that the provider ran that commit. A resumed run with no
new rows records zero input for that attempt rather than claiming fresh ingestion.
Consumption is explicitly `not_observed` until an authorized billing/usage source
is supplied; elapsed time is not a cost measurement.

Acceptance still requires the provider results, dependency resolution, real
Kafka connectivity, causal parity, table reconciliation and a failure/restart
exercise to be reviewed. The prepared code and a terminal `SUCCESS` alone do not
close the full project milestone. Databricks rollback is also open: run a second
explicitly compatible model version in its own qualification namespace, prove
its output, then select the previously known version and observe restored
predictions. No alias changes occur automatically on drift.

## Migration and recovery boundaries

Do not delete or repurpose a checkpoint while retaining its output namespace.
The deployment manifest rejects a changed model, source, configuration or code
identity against the existing namespace. The checkpoint marker also rejects
reusing that checkpoint for a different namespace, even if its Delta tables do not
yet exist. Model/config changes need a reviewed new namespace and checkpoint or an
explicit migration, with preserved old evidence. Keep the previous model and its
source/config dependencies available until rollback has been exercised.

The provider correction adds immutable alert decisions to prediction payloads.
Pre-correction predictions without those decisions are not silently upgraded or
reinterpreted. The changed source manifest requires a new isolated namespace and
checkpoint under the existing deployment guard; preserve the previous namespace
as evidence. No in-place provider migration is implemented by this change.
Legacy checkpoints without the ownership marker are not automatically adopted.

Never change a topic's partition layout in place for this qualification: it can
change a machine's arrival order. Use a new topic, checkpoint and output prefix,
then compare invariants on a fixed replay dataset. No cross-table exactly-once
guarantee or distributed scalability claim is made by this adapter.

## Local adapter regression evidence

The follow-up review reproduced three invalid-label accounting mismatches, a tied
availability ordering mismatch, and a delayed-label example that reported a
suppressed prediction as an alert. These were pure Python counterexamples against
the previously delivered source, not failures observed on a Databricks workspace.

The focused [adapter tests](../tests/test_databricks_adapter.py) now exercise those
cases, retained cooldown decisions, reconstruction after an explicit in-memory
prediction/alert-write boundary, transport-delayed outcomes, positive lead-time
metrics, dependency pin consistency and the guard against treating local Spark as
Databricks. An explicit in-memory filesystem double covers checkpoint creation,
ownership, initialization retry, missing namespaces, invalid paths and failed
reads/permissions. These filesystem cases do not contact DBFS or a cloud storage service.
The numerical import/ABI smoke executes against the clean locked local Python 3.11
environment, including rejection of stale loaded modules. Run the focused checks with:

```bash
MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true \
  uv run --frozen --python 3.11.18 pytest -q tests/test_databricks_adapter.py
```

The focused suite covers 37 cases, including 14 filesystem-double cases and four
native-interface guard cases. The latter use an explicit double or an SDK fallback
object created without its authentication/client constructor. No test
executes Spark, Delta, Kafka, a registry or a Databricks workspace; no provider
acceptance gate is closed by these results. Actual Delta partial-write recovery,
runtime library resolution and provider business-output parity remain required.

## Official references

- [DBR 15.4 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts)
- [Kafka source](https://docs.databricks.com/aws/en/connect/streaming/kafka/)
- [foreachBatch guarantees and full-batch consumption](https://docs.databricks.com/aws/en/structured-streaming/foreach)
- [Bundle settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Job tutorial and CLI requirements](https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial)
- [Compute permissions](https://docs.databricks.com/aws/en/security/auth/access-control)
- [Documented dbutils filesystem operations](https://docs.databricks.com/aws/en/dev-tools/databricks-utils)
- [SDK runtime selection source](https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/runtime/__init__.py)
- [Existing-cluster Jobs API task](https://docs.databricks.com/api/jobs/v2/run)
- [SciPy Python/NumPy support table](https://docs.scipy.org/doc/scipy/dev/toolchain.html)
- [Arrow 16 NumPy 2 compatibility notes](https://arrow.apache.org/blog/2024/04/20/16.0.0-release/)
