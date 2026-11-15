# Industrial Streaming MLOps

**Traceable maintenance warnings from sensor events, with an explicit false-alert budget.**

This synthetic industrial reference connects actual Kafka ingestion, Spark Structured Streaming, causal features, model evaluation, an HTTP MLflow registry and recovery. An alert retains its machine, sensor window and model identity. Local Spark execution is not Databricks provider acceptance.

## Current delivery status — 15 November 2026

The **local container reference has executed successfully on CircleCI**. The earlier starter pipeline is not the only evidence: the native Linux VM qualification built and ran the real services, verified the final data state, exercised recovery, promotion and rollback, and removed its owned resources.

| Gate | Observed result |
|---|---|
| Python unit/contract suite | 294 tests passed in the reviewed CI suite |
| Actual Kafka/Spark/MLflow deployment | Passed, source `4bcc55ec470a752bac59dfa45f561dad0261d95e` |
| Final state, offsets and scoring lineage | 20/20 final checks; 4,380 raw events, 2,092 eligible feature rows/scores, 12 alerts |
| Restart/recovery and model lifecycle | Recovery and lifecycle assertions passed |
| Dashboard HTTP endpoint | Reached by the container qualification; this is not a rendered-browser accessibility assessment |
| Databricks / Unity Catalog / provider infrastructure | Not accepted against an authorized live workspace |
| Repository licence | Not selected; no new licence grant is implied by publication |

[Full qualification and remaining acceptance criteria](docs/acceptance.md) · [Retained evidence](docs/evidence/) · [Current CircleCI reference run](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c)

This remains a reference candidate, **not an unqualified production or full-provider V1 release**. The application version is unchanged; no stable `v1.0.0` tag is asserted.

## Reproduce the local reference

Requirements: Linux Docker Engine with Compose, enough memory for the Kafka/Spark/MLflow services, and unused local ports 5000, 8080 and 19092. The qualification script refuses an existing evidence destination and records the source revision.

```bash
git clone https://github.com/mehdisafer/industrial-streaming-mlops.git
cd industrial-streaming-mlops
export COMPOSE_PROJECT_NAME="industrial-qualification-$(date -u +%Y%m%d%H%M%S)"
export SOURCE_REVISION="$(git rev-parse HEAD)"
export EVIDENCE_DIR="evidence/qualification-$(date -u +%Y%m%d%H%M%S)"
bash scripts/integration.sh
```

Do not create `EVIDENCE_DIR` in advance. The script owns its isolated stack and cleans up that stack, not unrelated Docker resources. A failure is retained, not converted to a successful receipt.

For source-level development, use the reviewed Python/uv lock:

```bash
python -m pip install 'uv==0.13.6'
uv sync --frozen --all-extras --python 3.11.18
uv run --frozen python -m compileall -q src databricks scripts
uv run --frozen ruff check src tests scripts databricks
uv run --frozen pytest -q -m 'not integration'
```

Install bootstrap tools in a virtual environment when the host Python is externally managed. The CI workflow does so instead of overriding system package protection.

## Runtime and network boundaries

Kafka, MLflow and the dashboard expose only loopback-bound host endpoints. They also join an ingress bridge so that those published ports are actually reachable on current Docker. Worker, CLI, topics initialization and state initialization retain internal-only networking.

**The three ingress-connected services can initiate outbound traffic. The whole deployment is not air-gapped.** PostgreSQL is not part of this stack. No production credentials, customer telemetry or cloud inference were used in these runs.

Raw events, causal windows, model selection, output identity and restart behavior are separate contracts. Event-time discipline and future-label isolation are tested. A synthetic holdout result is not a maintenance-effectiveness claim on an actual plant.

## CI profiles

`.circleci/config.yml` is the fast Smarter Testing build/test/lint pipeline. `.circleci/reference.yml` is a separate, manually triggered full Docker qualification on `ubuntu-2404:2026.10.1`. The existing `.github/workflows/ci.yml` retains its source checks and full integration path. There is no automatic provider deployment.

A green fast job does not substitute for the reference job or live Databricks acceptance. Original failed runs remain visible. See [compatibility](docs/compatibility.md), [data contracts](docs/data-contracts.md), [architecture](docs/architecture.md), [model card](docs/model-card.md), [operations](docs/operations.md), and [Databricks deployment/acceptance](docs/databricks.md).

## Evidence and publication

The complete historical native measurements, profiling, chronological validation, restore experiments and negative results remain in [the versioned evidence directory](docs/evidence/). The [acceptance record](docs/acceptance.md) distinguishes those historical runs from the hosted container run and identifies the exact qualified revision.

The remaining provider gate requires an explicitly authorized Databricks workspace, its permissions and an agreed resource budget. A licence decision and repository visibility are separate maintainer decisions. Neither is inferred from CI success.
