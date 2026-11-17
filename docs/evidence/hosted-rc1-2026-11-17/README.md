# RC1 source-bound hosted integration — 17 November 2026

This is a **read-only independent reinspection of original downloadable GitHub Actions artifact ZIPs**, not a re-execution of a workflow or a replacement for CircleCI. The executable source is exactly [`2a75cb05`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/2a75cb05c27d3303b90daab8d6424eb2c7a0e2b6), with `0.1.0rc1` version in the source, editable lock and importable module. [GitHub Actions run 44173769693](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/44173769693) completed **checks**, **real-integration** and **real Chrome** with success on this source.

[Manifest](manifest.json) records the original ZIP and selected member SHA-256, source receipts, independently checked semantics and limitations. **Raw original ZIPs are not committed**, as their logs may contain runtime details.

## Original ZIP artifacts and reproducibility

| Artifact | Original file count | ZIP size | Original ZIP SHA-256 |
|---|---:|---:|---|
| [Real Docker integration](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/44173769693/artifacts/14369341669) | 57 | 162,345 bytes | `a0a847f9c75c69155d817d51351e29988fde7ca87ca79da3d48887b4ae89ab78` |
| [Python and security](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/44173769693/artifacts/14368946478) | 5 | 15,252 bytes | `e66d38142e99ff0e190d3082b92489e44e9d533715362b3db63be39b5c7592b9` |
| [Chrome screenshots and receipts](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/44173769693/artifacts/14367949523) | 19 | 218,952 bytes | `fb1eccb7fc9c869185f4d62ff6fe911c515f431c8c81b0b4d7f3ef1fdcd15e7c` |

Each ZIP was downloaded from the original run and inspected in isolation: all CRC checks succeeded; no duplicate or unsafe member paths; all three original source-marker files agree on `2a75cb05`. The original ZIP SHA-256 values above also bind all other contents, not merely the subset of receipt hashes quoted in the manifest.

An operator can re-download the original artifacts while retained, compare whole ZIP SHA-256 (e.g. `sha256sum downloaded.zip`), validate ZIP CRCs and compare the selected member SHA-256 in the manifest. An artifact whose bytes differ from the recorded whole-ZIP hash **does not pass** this historical provenance test.

## Real Docker Kafka–Spark–MLflow protocol on RC1

The original `run-receipt.json` records stage `complete`, exit code 0, Docker stack reached, captured inventory complete, and cleanup exit code 0. Both `verification.json` and `final-verification.json` independently contain **all six named persisted-business checks true**, including SQLite integrity and source disposition/label/features/prediction/alert reconciliation.

| Observation | Before fresh rollback input | Final after rollback |
|---|---:|---:|
| Raw source records | 2,317 | 2,513 |
| Features / predictions | 1,141 / 1,141 | 1,225 / 1,225 |
| Alerts | 7 | 7 |
| Persisted invariants | 6/6 true | 6/6 true |

The difference is exactly **196** new acknowledged messages from the original `rollback-producer.json`. Final source disposition reconciles as **1,249 sensors + 1,249 labels + 8 controls + 6 rejected + 1 duplicate = 2,513 records**. Final predictions reflect 1,141 scores from numeric model version 2 and 84 from version 1; original `lifecycle-assertions.json` confirms a rollback. No persisted predictions/events remain pending, and the original Spark receipt reports zero Kafka lag across three partitions.

The three captured runtime inventories (JAR manifest, OS packages and Java version) were independently checked against their original size and SHA-256 receipts, as were original service-evidence duplicate files. The `cleanup.log` confirms containers and owned networks removed, with volumes intentionally retained. This is a **single-host synthetic reference**, not production capacity, multi-host recovery or Databricks.

## Tests, source security and browser

The original `tests.xml` has **355 passing pytest test cases**, zero failed, zero errored and zero skipped. The checked source/CI receipts agree on the same commit.

The original security inventory reports 359 worktree files and 508 reachable Git blobs scanned with **zero matching secret patterns**. The dependency JSON has 123 entries with **zero reported vulnerabilities**. Four license-metadata reviews remain flagged: `colorama`, `huey`, `pywin32`, `waitress`. These source-pattern and advisory observations are **not** a complete security or redistribution audit. PR #20's separate gzip scan executes in CircleCI; this GitHub Actions checks archive does **not** contain CircleCI's `archive-inventory.json`.

Three original Chrome screenshots and paired HTML/JSON receipts independently match their recorded hashes, dimensions and passed geometry checks: healthy desktop 1440×1000, healthy narrow 600×850 and unavailable desktop 1440×1000. The narrow table has its own horizontal scroll area; the document does not overflow horizontally. These renderings use the actual dashboard HTML with **synthetic API fixture data**; the screen's 4,380 sample records are **not** the 2,513 records in this Docker run.

## Explicitly independent gates

- CircleCI is the designated CI authority. Its [three mandatory contexts on `2a75cb05`](https://github.com/mehdisafer/industrial-streaming-mlops/commit/2a75cb05c27d3303b90daab8d6424eb2c7a0e2b6/checks) succeeded. The historical CircleCI `reference-containers` job **#13** original artifact bytes remain inaccessible without authorized CircleCI artifact access, and its invalid historical SHA-256 strings remain invalid.
- Live Databricks/Unity Catalog model, Delta, storage and Kafka qualification is **not executed**. Local Docker does not close it.
- Redistributed image layers, JVM connector JARs, optional Windows components and all applicable third-party rights remain separate.
- GitHub repository description/topics, `main` ruleset, tag `v0.1.0-rc.1` and GitHub prerelease are owner-side **not completed** as of the audited source revision.
- An original ZIP readback by this assisted audit is not independent human maintainer sign-off or production certification. No secrets, raw logs, billing changes or provider resources were published or created.
