# Acceptance record

Updated 15 November 2026. This file is the current acceptance decision, not a claim that every historical run passed.

## Decision

**The synthetic local container reference is qualified. Full provider V1 acceptance is still open.** The actual Databricks/Unity Catalog deployment, rendered dashboard qualification, maintainer licence choice and public visibility are separate items. Do not close the overall acceptance issue or label a stable production release based on the fast CircleCI job.

## Executed hosted qualification

Qualified runtime revision: `4bcc55ec470a752bac59dfa45f561dad0261d95e`.

CircleCI pipeline: `f18568b2-024d-4ca5-a586-57d150a0d5f2`.
Workflow: [reference-qualification](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c).
Job: `424ad872-14fc-4952-ba99-970ee84cf7d7`.

This was a native Linux VM run of `.circleci/reference.yml`, not a simulated Docker test or a replacement of provider acceptance. Original artifacts were downloaded and inspected before this record was written.

| Observation | Result |
|---|---|
| Build and real services | Kafka, Spark Structured Streaming, HTTP MLflow and dashboard executed |
| Run receipt | `status=passed`, `exit_code=0`, owned stack recorded |
| Final verification | 20 checks, zero failed checks |
| Persisted business data | 4,380 raw events; 2,092 feature rows; 2,092 scores; 12 alerts; 38 delayed labels |
| Output correctness | No duplicate raw-event, feature or score identity; scores reference known models; offsets and checkpoint exist |
| Recovery | Restart assertions true; no duplicate effect; expected bounded recovery output |
| Model lifecycle | Rejected candidate, champion retention, accepted candidate and rollback assertions true |
| Runtime provenance | Image, Java/JAR, OS-package and source evidence retained |
| Cleanup | Script recorded its owned cleanup; unrelated Docker resources were not pruned |

The source check profile separately executed **294 Python tests**, compilation, lint, security inventory and dependency checking. It is not counted again as part of the final 20 data checks.

## Retained-artifact integrity

The following SHA-256 values were computed from the downloaded, unmodified CircleCI artifacts:

| Artifact | SHA-256 |
|---|---|
| `integration/run-receipt.json` | `78b45855d20b1ab5c6784f59eff21dd8e42f2c0dedc345f8383cfc19312d148a9` |
| `integration/final-verification.json` | `5026ba8908142f6e21ca83a1ff2a03be6be63d9df3bb6f348a02723463d4383dd` |
| `integration/lifecycle-assertions.json` | `c9d805ba5503796db98a688e9b593de147744d2019455fe11b8d659811eb3c0de` |
| `integration/recovery-assertions.json` | `66589288bdba4bb79b6068baab056fa822fa1545df851779f407ce62e7ec42512e` |

The manifest and this record are summaries of execution evidence; they are not new execution outputs. CircleCI artifact retention is finite. Existing permanent native evidence remains under [evidence/](evidence/), with its own revision and environment.

## Corrections established by this qualification

1. The Actions-compatible runner exposed an externally managed system Python. Bootstrap uv now runs in a disposable virtual environment rather than bypassing system protections.
2. The native wrapper initially pre-created the immutable integration destination. Host metadata now goes to `evidence/ci-host`; the integration script creates its own fresh destination.
3. Internal-only Docker networks did not realize the published host endpoints in the observed environment. Kafka, MLflow and dashboard now join a separate ingress bridge. Host bindings remain `127.0.0.1`; worker, CLI and initialization remain internal-only.

The ingress-connected services can initiate egress. This is **not** a whole-stack air-gap guarantee. The public bindings were not widened to `0.0.0.0` to obtain a pass.

The failed setup, wrapper and connectivity runs remain failures in history. A later corrected run does not retrospectively validate them.

## Reproduction and CI separation

The fast configuration is `.circleci/config.yml`; it uses the repository's locked environment and Smarter Testing. The manual full configuration is `.circleci/reference.yml`, on `ubuntu-2404:2026.10.1`, with a bounded execution and an isolated Compose project. The existing GitHub Actions workflow remains available. No cloud deployment is embedded in these profiles.

Run `bash scripts/integration.sh` with a unique `COMPOSE_PROJECT_NAME`, `SOURCE_REVISION=$(git rev-parse HEAD)` and a non-existing `EVIDENCE_DIR`. Consult [operations](operations.md) for normal operation and [compatibility](compatibility.md) for the pinned stack. Cleanup must remain scoped to the run's recorded project.

## Open gates

| Gate | Required evidence to close |
|---|---|
| Databricks and Unity Catalog | Actual authorized workspace validation, deployment, execution, permissions, lineage, promotion/rollback and retained provider outputs; local Spark is not a substitute |
| Rendered dashboard | Actual browser interaction/rendering evidence; successful HTTP and JSON checks alone do not qualify visual behavior |
| Licence | Explicit maintainer selection and a corresponding licence file; no terms invented here |
| Publication | Maintainer-confirmed public visibility and applicable branch protections |
| Operational production acceptance | Named target, limits, credentials, restore/recovery exercise and operator acceptance in that target |

Historical local measurements, larger profiles and earlier diagnostics remain available in [evidence/](evidence/). Their exact previous acceptance text can be inspected in the Git history at runtime revision `4bcc55ec470a752bac59dfa45f561dad0261d95e`; they must not be mistaken for the hosted run above. No stable `v1.0.0` tag is asserted by this record.
