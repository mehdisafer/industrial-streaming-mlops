# Original hosted reference qualification — 17 November 2026

This directory records a **read-only reinspection of the original GitHub Actions ZIP archives** from [run 43983190446](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43983190446), on exact source [`185c415`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/185c41508c937addb39b9c827a817f67e5b8519f). That source includes the maintainer-approved Apache-2.0 licence.

The run's three jobs (`checks`, real Docker `real-integration`, and real Chrome) completed successfully. The Docker job executed `scripts/integration.sh` with Kafka, Spark Structured Streaming and the HTTP MLflow registry; the run receipts are not inferred from a green status alone. [manifest.json](manifest.json) retains exact original ZIP digests, source identity, selected original-file digests and the bounded semantic checks independently performed.

## Original archive identities

| Archive | GitHub Actions artifact | Source ZIP size | Files | Original ZIP SHA-256 |
|---|---|---:|---:|---|
| Integration | [14283867927](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43983190446/artifacts/14283867927) | 163,224 bytes | 57 | `07b68e1d6c9fe4eaa1696ece71711250868885e13882d15c75953b99dee0a229` |
| Checks | [14284166574](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43983190446/artifacts/14284166574) | 14,887 bytes | 5 | `60f13fd0fd7e52209378eed04da295302601fd53c0da0c4a0a46ae4897b09da3` |

The archives were downloaded from the authenticated repository connector and inspected in an isolated read-only environment. All ZIP CRC checks succeeded, no duplicate or unsafe member paths were found and both `commit.txt` files match `185c415`. Selected receipt hashes and all three runtime inventory SHA-256 digests were recomputed; original raw logs have **not** been copied into this public repository.

## Source and business outcomes

| Stage | Raw records | Predictions | Alerts | Named persisted checks |
|---|---:|---:|---:|---|
| Initial service verification | 2,317 | 1,141 | 7 | All 6 true |
| Post-rollback final verification | 2,513 | 1,225 | 7 | All 6 true |

The difference is **196 new records**, independently matched to the `rollback-producer.json` receipt. Final dispositions reconcile to 2,513 raw records: 1,249 accepted sensors, 1,249 labels, 8 controls, 6 rejects and 1 duplicate. The same 1,225 feature and prediction rows agree, seven alerts refer to committed predictions, model versions `1` and `2` account for 84 and 1,141 predictions, no work is pending and final Kafka lag is zero across three partitions. The lifecycle receipt records observed rollback and the recovery receipt confirms committed progress.

The integration run receipt records `stage=complete`, `exit_code=0`, a complete JVM/package inventory and `cleanup_exit_code=0`. Original `cleanup.log` records containers and the two owned networks being stopped/removed. Volumes were intentionally preserved. These are scoped observations from a **single-host synthetic reference**, not multi-node high availability or provider acceptance.

## Python, security and licensing observations

The original `tests.xml` contains **333 tests, zero failures, zero errors and zero skips**. The dependency JSON lists 123 audited entries with no vulnerability records reported. The pattern-based source scanner reports 351 worktree files, 462 reachable Git blobs and zero matched secrets; it does **not** inspect all binary, unreachable or external surfaces.

The metadata inventory contains 126 locked components (three not installed on this runner). Four entries require licence-metadata follow-up: `colorama`, `huey`, `pywin32` and `waitress`; only `huey` is installed here. A missing declaration is **not proof of an incompatible licence**. The project itself is Apache-2.0, while upstream components retain their independent rights and redistribution obligations.

## What remains distinct

- **CircleCI continues as the required hosted CI.** Its original `reference-containers` job #13 succeeded on `4bcc55e`, but raw bytes behind that older job's artifact links still require authorized retrieval. The four 65–66-character strings once presented as SHA-256 are invalid; this new GitHub Actions archive does not repair or authenticate them.
- **Databricks/Unity Catalog is not validated.** The provider requires an explicitly authorized existing workspace, compute, storage, Kafka secret and model access.
- **No complete security, third-party legal clearance, real industrial accuracy or production-readiness claim** follows from this audit.
- GitHub Actions is used here as **additional retained evidence from a completed run**, not as a mandatory CI gate or a request to increase Actions quota.

To reproduce, download the two original artifacts above while retained, compare their whole-archive SHA-256 and verify the selected file digests and stage invariants in [manifest.json](manifest.json). Keep the downloaded archive private if its logs contain runtime details. The manifest is a record of an assisted recheck, **not independent human sign-off**.
