# Real Chrome dashboard evidence — 16 November 2026

The original [Chrome run 43921124746](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746)
uploaded [artifact 14253962621](https://github.com/mehdisafer/industrial-streaming-mlops/actions/runs/43921124746/artifacts/14253962621).
The original ZIP was downloaded, its **19 members** inventoried, and each scenario's
original PNG and JSON bytes independently SHA-256 rehashed. The three PNGs were
visually inspected. Chrome version: **157.0.8103.50**.

The [manifest](manifest.json) records the source identity, original run and artifact,
source file hash, exact screenshot/report hashes, and the test boundaries.
The tested dashboard Git blob is identical on `main`.
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
