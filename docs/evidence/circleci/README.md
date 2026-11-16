# CircleCI original artifact intake

The [reference-containers job](https://app.circleci.com/workflow/a1ff1428-da21-4550-afe2-8a7dc55d055c/job/424ad872-14fc-4952-ba99-970ee84cf7d7) reports success on source `4bcc55ec470a752bac59dfa45f561dad0261d95e`. The current repository preserves that observed status, **not a revalidated artifact archive**. No CircleCI account token or Docker runtime is needed by the offline checker.

## Historical hash problem

The four strings recorded in the earlier acceptance note as SHA-256 have **65, 65, 65 and 66 hexadecimal characters**. SHA-256 digests contain exactly 64. Those strings are kept in [manifest.json](manifest.json) strictly as historical, **invalid** claims. They are not expected digests and must not be truncated or guessed.

## Evidence collection and verification

1. From the successful CircleCI job's **Artifacts** tab, download the original `integration/` files and `ci-host/` metadata without modifying them. Keep the directory structure. The job's artifact availability/retention is controlled by CircleCI.
2. Inspect source identity, project, successful cleanup and data assertions. The repository's `Store.verify()` currently emits **six** named checks; the intermediate note's "20 final checks" total cannot be substituted for the structure of `final-verification.json` without inspecting the originals.
3. Run the checker on isolated copies, not on an active project or service:

```bash
python scripts/verify_circleci_artifacts.py \
  --artifacts /path/to/download/integration \
  --host /path/to/download/ci-host \
  --full \
  --output /path/to/new/verification-report.json
```

The first read with `manifest.json` containing an empty `expected_sha256` can only produce `observed_unpinned` when semantic checks pass. That is **not** an integrity PASS. A failed receipt or a missing artifact produces `failed`.

After separately reviewing the original download and its source/run identity, enter the **actual 64-character** computed SHA-256 for each of the four receipts in `manifest.json` under `expected_sha256`; commit the corrected manifest through a reviewed PR, then rerun the offline checker against the independently retained original files. Only then can a `verified` result establish **this bounded receipt and inventory integrity scope**. It does not by itself qualify a Databricks workspace, security, production operations or an independent human review.

The source [fixture tests](../../../tests/test_circleci_artifact_verifier.py) use synthetic receipts and never pretend to execute Kafka, Docker or Databricks. The checker neither connects to CircleCI nor edits the original artifacts. Its optional `--output` is created exclusively and will refuse an existing path.
