# Local operation, incidents and restoration

The Compose worker defaults to a **300-second run** and `restart: no`. Select `--duration-seconds` or `--available-now` and retain original results/logs. A finite timeout does not prove Kafka drained.

## Observe before changing state

The dashboard is at `http://127.0.0.1:8080`; MLflow is at `http://127.0.0.1:5000`. `/api/snapshot` exposes pipeline state, recent alerts, mature labels and rejection reasons. `/metrics` exposes database counters. `/health` verifies persisted invariants; **it does not certify freshness, broker availability or model quality**.

Inspect Spark source offsets/progress, acknowledged producer counts, throughput, batch age, pending predictions, active model/version, rejection reasons and measured inference/end-to-end latency. Synthetic event time and wall-clock processing time have different meanings. Qualification requires balanced source dispositions, complete expected input, compatible inference and recovery without duplicated effects. The [model card](model-card.md) states the predeclared alert budget and quality gates.

| Symptom | Action and recovery evidence |
| --- | --- |
| Worker/broker interruption | Preserve topic, checkpoint and database. Restore service, resume the same binding and reconcile coordinates/counts. Do not delete checkpoints to clear an error. |
| Rising backlog or timeout | Check source offsets, batch duration and row/byte bounds. Reduce the workload or trigger size. Record the change; more local workers do not remove SQLite's single-writer limit. |
| Poison/schema/late records | Inspect the reason and raw coordinate; fix the producer or create an identified correction replay. Do not relabel rejects as accepted. |
| Model unavailable/incompatible | Keep accepted data and pending features. Restore the exact trusted version, then run `score`; verify the queue drains without default healthy scores. |
| Drift/quality deterioration | Inspect automatic post-batch monitoring or run `monitor`. Metrics count actual persisted alerts with mature labels; cooldown is not reconstructed at a rolling boundary. Investigate inputs; never promote from drift alone. |

## Model changes

`train` evaluates and registers. `promote --version N` changes the registry's `champion` pointer after gate checks. `activate --version N` records the worker's numerical deployment separately; unqualified demonstrations require explicit `--shadow`. Keep those outputs marked exploratory.

`rollback` activates the previous local deployment after compatibility checks. It does not undo existing predictions or automatically change the registry alias. Observe subsequent predictions carrying the restored version and compare the deployment history; a command return alone is not a rollback demonstration.

## Backup and restore

Stop producers, worker, dashboard, MLflow and Kafka before taking a coordinated snapshot. `--quiesced` is an operator assertion, not an automatic service-stop check. Preserve **both** Compose volumes: `state` contains pipeline/MLflow/checkpoints/artifacts; `kafka-data` contains the broker log. The backup CLI copies only the directory supplied; it does not discover or snapshot the second volume automatically.

Export both stopped volumes into a host-owned snapshot root, preserving their relative layouts. Record image/source identities and configuration alongside it. Then use the application copy/check procedure:

```bash
industrial-mlops backup --state-dir stopped-snapshot \
  --output backups/rehearsal --quiesced
industrial-mlops restore --input backups/rehearsal --target restored-rehearsal
```

The destination must be new. Restore rejects path traversal and symlink ancestors, verifies source and copied content, and **does not start services**. Hashes do not prove recovery. Mount restored data at the original in-container paths on an isolated stack, verify the database, load the model, resume retained Kafka input, and compare counts/predictions with pre-backup evidence.

Checkpoint bindings contain absolute sink paths, source and configuration. Preserve those service paths; do not hand-edit bindings to suppress a mismatch. A changed path/configuration needs an explicit new-store/checkpoint migration and retained-input replay. The native restoration exercise makes one controlled exception for an unchanged, hash-verified stopped-state copy: it checks the exact original database path, changes only that path in the copied binding, and resumes the copied checkpoint against the restored database and broker. The original binding remains intact. This is verified-copy relocation, not permission to combine unrelated stores and checkpoints. Only the initial schema is implemented; no general in-place migration is claimed. Keep the old snapshot until the restoration exercise has passed. Broker data must still cover all unreconciled offsets; expired history cannot be reconstructed from a checkpoint.

## Rehearse restored backlog and continued processing

The [15 November exercise](evidence/followup/README.md#restored-backlog-and-fresh-input-proof)
qualified a stopped single-host copy with unconsumed Kafka messages, followed by
new publication after restore. It checks original labels, event payloads, features,
predictions, alerts, configuration and deployment history while allowing mutable
finalization, watermarks and runtime health to advance.

After installing the pinned native prerequisites, select a new work directory:

```bash
export MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true
GITHUB_SHA=$(git rev-parse HEAD) uv run --frozen --python 3.11.18 \
  python scripts/qualify_restore.py \
  --work-dir state/restore-rehearsal-001 --machines 6 --budget-seconds 600
```

The original work, backup and restored destinations must all be new. The runner
owns the services on loopback ports 5000, 8080, 19092 and 19093, reserves 180 seconds
of its total budget for cleanup, and records process-group/port observations before
backup and after completion. A failed or interrupted business protocol cannot pass
merely because cleanup succeeded. Keep its protocol, reports, inputs and logs.

In the recorded run, 303 retained messages and 303 new messages were consumed;
1,965 predictions remained with no pending work or observed lag. The final empty
resume did not duplicate historical business rows. This exercise does not qualify
cross-host recovery or recovery after the required broker offsets have expired.

