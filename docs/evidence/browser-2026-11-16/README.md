# Real Chrome dashboard evidence — 16 November 2026

The original [Chrome run 43921124746](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746)
uploaded [artifact 14253962621](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746/artifacts/14253962621).
The original ZIP was downloaded, its **19 members** inventoried, and each scenario's
original PNG and JSON bytes independently SHA-256 rehashed. The three PNGs were
visually inspected. Chrome version: **157.0.8103.50**.

The [manifest](manifest.json) records the run identity, code and tree provenance,
exact screenshot/report hashes, and test boundaries. A fresh isolated recheck
on **17 November 2026** computed ZIP SHA-256
`319c160ff552126141f73968747190bb02ee54269d827818435a14b4904f1525`
for the original **218,970-byte, 19-entry** archive and reconfirmed all three
PNG hashes, three JSON hashes, the summary hash, dimensions and expected
browser receipts. This is an assisted evidence recheck, not independent human
review or renewed production testing.

GitHub Actions run `43921124746` identifies PR head `2148c9f`, while the actual
`source-commit.txt` inside the ZIP says `d7ee564` because GitHub checked out its
temporary **pull-request merge commit**. GitHub commit metadata confirms the
merge commit has parents `580e130` and `2148c9f`. Critically, both the PR head
and temporary merge commit have the **same complete Git tree SHA**,
`4641b61794ea5d3dd4af01fc7459ee17a6431200`. The checkout identity differs
without a difference in tested source content. The tested dashboard Git blob is
also identical on `main`.
This is evidence from the actual application HTML presented with a **synthetic
local API fixture**, not a live streaming-service screenshot.

An initial narrow rendering revealed a status badge overlapping its subtitle.
The corrected CSS and DOM rectangle assertion passed on the final 600 px image.
The earlier sandbox-blocked Chromium and first feature-ID assertion failure remain
historical unsuccessful attempts.

To reproduce, on a suitable non-root Linux host with sandboxed Chrome and a
fresh output location:

```bash
DASHBOARD_BROWSER_EVIDENCE=evidence/browser-new bash scripts/dashboard_browser_smoke.sh
```

Separate Kafka/Spark/MLflow container acceptance is recorded in
[the acceptance register](../../acceptance.md). This browser artifact does
**not** certify live-data integration, accessibility, operator signoff,
Databricks, licence choice or production operation. Hosted artifact retention
may expire, so this manifest preserves the exact original hashes without
claiming to store the screenshots in Git.
