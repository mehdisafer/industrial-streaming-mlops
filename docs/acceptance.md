# Acceptance and remaining work

Implementation, execution and acceptance are separate states. Release evidence must attach the actual commit, inputs, versions, commands, outputs and limitations. A configured job, skipped test or started service does not close an integration gate.

| Criterion | Review state | Required acceptance evidence |
| --- | --- | --- |
| Strict contracts, causal features, delayed labels | Tested in application suite | Contract/temporal tests and versioned inputs |
| Sink atomicity and replay | Tested with SQLite and a unit predictor | Before/after-commit reopen tests; real Spark restart receipt remains separate |
| Chunk/partition invariance | Tested in application suite | Same feature bodies across chunk sizes and partition mappings |
| Model failure, identity, policy and cooldown | Tested with explicit unit doubles | Live registry/inference and activation evidence |
| Rolling monitoring | Tested with SQLite/unit predictor | Persisted-alert cooldown, future-label exclusion and no automatic promotion; service receipt separate |
| Kafka → Spark → MLflow → dashboard | Implemented; integration evidence pending attachment | Real source offsets, registry versions, nonzero predictions, dashboard output and reconciliation |
| Broker/worker failure and model rollback | Implemented exercise paths; service qualification open | Observed interruption, resumed processing and restored numerical model version |
| Backup/restoration | Implemented copy/integrity workflow | Restarted restored stack, model loading and retained-input reconciliation |
| Load and resources | Bounded protocols implemented | Identified environment, original measurements and unchanged business invariants |
| Databricks | Prepared; not executed | Authorized workspace/compute, provider run IDs, Delta versions, outputs and recovery proof |
| Public release | Not authorized or performed | Owner license decision, publication review and authorized visibility change |

The targeted data/sink command is reproducible:

```bash
uv run --frozen pytest tests/test_contracts.py tests/test_features.py \
  tests/test_simulator.py tests/test_store.py tests/test_observability.py -q
```

Tests use a named fake predictor where appropriate. The sink benchmark excludes Kafka, Spark and inference. Configured CPU/memory limits are not observed usage or cost. Real integration and provider receipts remain separate.

## Prioritized backlog

1. Attach service-chain, interruption, rollback, restoration and container receipts for the delivered commit; retain failures openly.
2. Run the [Databricks qualification](databricks.md) on explicitly authorized existing resources. Verify task-library compatibility, access, outputs and available consumption data.
3. Before larger deployments, implement durable distributed feature state, multi-node failure tests, retention/archival, authenticated service exposure and calibrated operational SLOs. The current driver/SQLite design does not establish these capabilities.
4. Add representative external data, unseen-site evaluation, calibration and uncertainty estimates before claiming industrial predictive value.
5. Obtain the repository license decision. Description/topics and branch protections require a supported authorized administration path; no unsupported connector mutation or paid plan is assumed. Review history, issues, PRs, logs and artifacts before any public release, then verify anonymous repository access. Business services remain private.

