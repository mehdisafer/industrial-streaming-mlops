# Data contracts and temporal semantics

The executable authority is [contracts.py](../src/industrial_mlops/contracts.py); readable schemas are in [schemas](../schemas). Configuration is validated by [PipelineConfig](../src/industrial_mlops/config.py). Unknown operations and schema versions are rejected rather than interpreted as code.

| Record | Identity and purpose |
| --- | --- |
| Sensor | `event_id`, dataset, source, site, machine, UTC event/ingestion time, schema version, temperature, vibration, pressure and explicit units |
| Label | Separate event identity, referenced sensor ID, target, failure time and availability time |
| Heartbeat | Reviewed control that advances a machine's watermark; never a model input |
| Feature | Anchor ID, source-event IDs, causal window values, availability time and semantic signature |
| Prediction | Feature identity, score, fixed threshold, model/run/version, feature signature and measured execution timing |
| Alert | Prediction provenance, observed values, evidence and cooldown outcome; mode distinguishes approved from shadow |

Units are °C (`degC`), mm/s and bar; default ranges are −40…180°C, 0…30 mm/s and 0…30 bar. Missing/nonfinite values, booleans, unexpected fields, invalid units, malformed JSON, invalid UTF-8 and Kafka tombstones are rejected with original source bytes preserved. Sensor payloads cannot contain labels or scenario truth.

## Window closure

Each dataset/site/machine has its own maximum observed event time. The default watermark is that maximum minus **120 seconds**. A sensor at or before the prior watermark is rejected as late. It remains in the raw ledger with its disposition; it does not rewrite finalized predictions. Correction requires a separately identified replay, not a silent update.

Accepted anchors close when their event time is at or before the updated watermark. Features use the same machine's observations in **(anchor − 30 minutes, anchor]**, already ingested by closure. Four distinct observations are required; warmup anchors remain accepted without a feature. Quiet machines retain pending anchors. The finite simulator's final heartbeats close them; labels never advance a watermark.

The eight model inputs are temperature mean/max, vibration mean/max, pressure mean, temperature/vibration slopes per minute and sample count. Dataset, machine/site identity and labels remain metadata. `available_at` is closure time, not anchor time. The feature signature binds ordered names, version, units, window, minimum points, lateness and forecast horizon. Budget and payload bounds have separate operating-policy checks.

## Labels and reproducibility

The target is failure in the next 30 minutes. Positive labels become available after failure; negative labels require the whole horizon to elapse. The simulator adds the configured label delay. The sink additionally requires an accepted sensor with matching dataset/site/machine/time and permits only one label per anchor. Unmatched or conflicting labels are rejected for investigation.

The generator manifest records parameters, seed, failure schedule, input hash and independently specified anomaly counts. Ground truth serves evaluation only. [Historical replay](../src/industrial_mlops/features.py) and the live sink share feature calculations and per-machine ordering; [tests](../tests/test_features.py) check leakage and partition invariance. Synthetic lead time is separate from processing latency: accelerated replay does not provide real-world advance warning.

