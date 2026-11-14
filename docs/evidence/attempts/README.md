# Preserved qualification attempts

This bundle retains the original failed attempts and the later approval block. It distinguishes model evaluation, committed partial pipeline effects, an aggregate qualification result and an approval rejection. The [evidence manifest](evidence-manifest.json) records original paths, exact-copy SHA-256 checks and the provenance of authored summaries. Runtime originals were not rewritten.

## Recorded outcomes

| Attempt | Observed outcome | Original or supplemental evidence |
| --- | --- | --- |
| 01 | Training and registration completed; aggregate run failed because the idempotent producer required `max_in_flight_requests_per_connection=1`. | [Original console log](native-01.log), [original training evidence](../model/README.md) |
| 02 | Aggregate run failed when the Java gateway exited before sending its port number. | [Original console log](native-02.log) |
| 03 | Aggregate run failed in the stream callback: strict base64 decoding rejected source data. | [Original console log](native-03.log) |
| 04 | Several recovery/parity checks completed and rollback-stage writes committed; aggregate run failed after the Java gateway disappeared. Later dashboard and full restoration stages were not completed in this attempt. | [Original console log](native-04.log), [Kafka log](native-04-kafka.log), [failure audit](native-failure-review.md) |
| 05 | Aggregate run failed on a 60-second `create_topics` timeout before any Spark worker started. Broker logs nevertheless show topic-partition creation during cleanup. | [Original console log](native-05.log), [Kafka log](native-05-kafka.log), [MLflow log](native-05-mlflow.log) |
| 06 | Automatic approval review rejected a tool request associated with external MLflow telemetry to `config.mlflow-telemetry.io`. No completed qualification result was observed; the first worker log was empty. | [Rejection receipt](06-approval-rejection.json), [SDK opt-out observation](telemetry-optout.json) |

The original reports for attempts 01–05 each record `passed: false`. The manifest includes their recorded failure and checks plus the original report hashes. Those full runtime reports are not duplicated here; the bundled console logs and audit preserve the relevant failures.

## Historical audit and later status

[native-failure-review.md](native-failure-review.md) and [native-failure-review.json](native-failure-review.json) are byte-for-byte copies of the original review of attempts 04 and 05. They were written while attempt 06 was still in progress. Their sentence describing 06 as in progress is historical; [06-approval-rejection.json](06-approval-rejection.json) supersedes that status with the observed approval block. This preserves the original audit without silently editing its history.

Attempt 04 retains real partial effects: 4,380 raw records, 2,092 features, 2,092 predictions, 12 alerts and seven batch-ledger entries. The recorded deployment sequence was version 1, version 2, then rollback to version 1. These effects are evidence of those completed stages, while the aggregate result remains failed.

## Telemetry remediation and proof limits

The parent applied the official MLflow opt-out before retrying: `MLFLOW_DISABLE_TELEMETRY=true` and `DO_NOT_TRACK=true`. The bundled [installed SDK check](telemetry-optout.json) records MLflow 3.17.2 and `telemetry_client_is_none: true`. Its cited official source is the [MLflow usage-tracking documentation](https://mlflow.org/docs/latest/community/usage-tracking/).

That check is an actual SDK observation with opt-out active. It is not a packet capture or a qualification result. The rejection receipt summarizes the parent agent's observed tool denial; a verbatim platform rejection transcript was not available in this bundle. The exact rejection time was not recorded, so the receipt creation time is not presented as the rejection time.

The telemetry rejection does not establish the cause of attempt 04's Java gateway loss, attempt 05's broker/client stalls, or the startup wait preceding attempt 06's rejection. Out-of-memory, CPU throttling and accumulated JVM explanations remain unproven. The original Kafka and MLflow timestamps are retained as logged; they have not been silently normalized to another time zone.

## Completed follow-up: attempts 07 and 08

Attempt 07 later completed its then-existing 19 checks and recorded `passed: true`, but its original worker receipts reported source lag unavailable with a `TypeError`. The report omitted a final lag result and did not require a zero-lag check. Its [original qualification report](native-07-qualification.json), [console log](native-07.log) and [lag receipt](native-07-lag-observation.json) are retained without rewriting that historical result. The [authored supersession note](07-superseded-note.md) explains the incomplete metric coverage.

The current completed receipt is [native08](../native/README.md): all 21 checks true, 4,380 raw records, 2,092 predictions, 12 alerts and observed source lag zero. It adds explicit checks for final zero lag and consumption of new input by the active worker after broker recovery. This supersedes attempt 07 for the full acceptance scope while preserving all earlier evidence.
