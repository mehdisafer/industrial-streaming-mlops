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
| `predictions` | Real registered-model inference, score, exact model version and evidence payload |
| `alerts` | Threshold and cooldown policy outcomes, linked to predictions |
| `batches` | Completion records written after the other outputs |
| `deployment` | Immutable source/config/model identity for the table/checkpoint namespace |

SQLite is not used on a cloud mount. Each table uses an idempotent Delta `MERGE`.
The tables do **not** form one atomic transaction. A failed batch may leave
partial tables visible; its completion record is absent and replay finishes the
remaining writes. A provider failure/restart exercise is still required before
claiming that recovery behaviour is verified on Databricks.

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

## Reproducible deployment and bounded run

Supply the variables as local environment variables (`BUNDLE_VAR_<name>`) or an
ignored local bundle override. They are identifiers and paths, not secret values.
Required variable names are listed in `databricks.yml`. Set `code_sha` to the
reviewed Git commit and upload a clean checkout of that commit. Use a distinct
`table_prefix`, `output_root` subdirectory and `checkpoint_root` for each intended
source/config/model deployment. Keep each machine on one Kafka partition.

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
identity against the existing namespace. Model/config changes need a reviewed
new namespace or an explicit migration, with preserved old evidence. Keep the
previous model and its source/config dependencies available until rollback has
been exercised.

Never change a topic's partition layout in place for this qualification: it can
change a machine's arrival order. Use a new topic, checkpoint and output prefix,
then compare invariants on a fixed replay dataset. No cross-table exactly-once
guarantee or distributed scalability claim is made by this adapter.

## Official references

- [DBR 15.4 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts)
- [Kafka source](https://docs.databricks.com/aws/en/connect/streaming/kafka/)
- [foreachBatch guarantees and full-batch consumption](https://docs.databricks.com/aws/en/structured-streaming/foreach)
- [Bundle settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Job tutorial and CLI requirements](https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial)
- [Compute permissions](https://docs.databricks.com/aws/en/security/auth/access-control)
- [Existing-cluster Jobs API task](https://docs.databricks.com/api/jobs/v2/run)
