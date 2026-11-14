# Attempt 07: superseded by completed attempt 08

The original [qualification report](native-07-qualification.json) records `passed: true` and **19 checks true**. That historical result is preserved exactly, together with the [original console log](native-07.log).

The report does not contain a final `source_lag` result or require `final_source_lag_zero`. The original final-worker [lag observation](native-07-lag-observation.json) records `source_lag.status: unavailable` and `exception: TypeError`. The same status is present in the original active-query and restored-checkpoint receipts. An unavailable metric cannot establish a zero backlog, even when other checks succeed. The original 19-check result is therefore insufficient for the complete acceptance scope now recorded by attempt 08; it has not been silently rewritten as a failed or fully validated historical report.

[Native08](../native/README.md) preserves a fresh completed qualification with **21 checks true**, an observed final lag of zero, and an explicit check that the active worker consumed newly published recovery input. The two additional checks are `final_source_lag_zero` and `active_worker_consumed_recovery_input`. Its final result is 4,380 raw records, 2,092 predictions and 12 alerts, with no pending events or predictions.

This note is authored analysis, recorded at 2026-11-13T23:53:54.993667+00:00. The original report, console log and lag receipt are byte-for-byte copies with source and destination hashes in [evidence-manifest.json](evidence-manifest.json). The TypeError observation is reported as recorded; no missing metric is imputed and no earlier result is erased.
