#!/usr/bin/env bash
# Bounded real-container smoke on an isolated Compose project.
set -euo pipefail
evidence_dir="$(python -c 'import os; print(os.environ.get("EVIDENCE_DIR", "evidence/integration"))')"
mkdir -p "$evidence_dir"
docker compose build --pull > "$evidence_dir/build.log" 2>&1
docker compose up -d --wait --wait-timeout 240 kafka mlflow dashboard
cli() { docker compose run --rm -T cli "$@"; }
read_json() {
  docker compose run --rm -T --entrypoint python cli -c "$1"
}
cli generate --output /state/input/events.jsonl --machines 4 --hours 24 --anomalies \
  > "$evidence_dir/generate.json"
cli train --output /state/training --machines 4 --hours 24 --report /state/evidence/training.json \
  > "$evidence_dir/train.log" 2>&1
baseline="$(read_json "import json; print(json.load(open('/state/training/training-report.json'))['models']['baseline']['model_version'])")"
candidate="$(read_json "import json; print(json.load(open('/state/training/training-report.json'))['models']['candidate']['model_version'])")"
eligible="$(read_json "import json; print(str(json.load(open('/state/training/training-report.json'))['models']['candidate']['qualification']['eligible']).lower())")"
cli activate --version "$baseline" --shadow > "$evidence_dir/activate-baseline.json"
if [ "$eligible" = true ]; then
  cli promote --version "$candidate" > "$evidence_dir/promotion.json"
  cli activate --version "$candidate" > "$evidence_dir/activate-candidate.json"
else
  cli activate --version "$candidate" --shadow > "$evidence_dir/activate-candidate.json"
fi
cli produce --input /state/input/events.jsonl --report /state/evidence/producer.json \
  > "$evidence_dir/producer.log" 2>&1
cli stream --available-now --duration-seconds 300 --report /state/evidence/stream.json \
  > "$evidence_dir/stream.log" 2>&1
cli verify --output /state/evidence/verification.json > "$evidence_dir/verification.json"
# Failure after an atomic sink commit, before Spark can advance its checkpoint.
cli activate --version "$candidate" --db /state/pipeline/fault.db --shadow \
  > "$evidence_dir/fault-activation.json"
if cli stream --available-now --db /state/pipeline/fault.db --checkpoint /state/fault-checkpoint \
  --query-id fault --failpoint after_commit --fail-batch 0 --fail-marker /state/fault.marker \
  > "$evidence_dir/injected-failure.log" 2>&1; then
  echo "Expected the injected worker failure to stop the query" >&2
  exit 1
fi
cli stream --available-now --db /state/pipeline/fault.db --checkpoint /state/fault-checkpoint \
  --query-id fault --report /state/evidence/recovery.json > "$evidence_dir/recovery.log" 2>&1
read_json "
import json,sqlite3
expected=json.load(open('/state/evidence/producer.json'))['acknowledged']
for name in ('monitor','fault'):
 db=sqlite3.connect('/state/pipeline/'+name+'.db')
 assert db.execute('select count(*) from raw_records').fetchone()[0]==expected
 assert db.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert db.execute('select count(*) from predictions').fetchone()[0]>0
print(json.dumps({'source_reconciliation':True,'after_commit_recovery':True,'acknowledged':expected}))
" > "$evidence_dir/recovery-assertions.json"
docker compose kill -s SIGKILL kafka
docker compose up -d --wait --wait-timeout 120 kafka
cli stream --available-now --report /state/evidence/broker-restart.json \
  > "$evidence_dir/broker-restart.log" 2>&1
cli rollback > "$evidence_dir/rollback.json"
cli generate --output /state/input/rollback.jsonl --seed 43 --machines 4 --hours 2 \
  --start 2026-06-01T00:00:00Z > "$evidence_dir/rollback-input.json"
cli produce --input /state/input/rollback.jsonl > "$evidence_dir/rollback-producer.json"
cli stream --available-now --report /state/evidence/rollback-stream.json \
  > "$evidence_dir/rollback-stream.log" 2>&1
cli monitor --report /state/evidence/monitoring.json > "$evidence_dir/monitoring.log" 2>&1
cli verify --output /state/evidence/final-verification.json > "$evidence_dir/final-verification.json"
read_json "
import json,sqlite3
db=sqlite3.connect('/state/pipeline/monitor.db')
versions=dict(db.execute('select model_version,count(*) from predictions group by model_version'))
assert len(versions)>=2
assert db.execute('select action from deployments order by id desc limit 1').fetchone()[0]=='rollback'
print(json.dumps({'real_prediction_versions':versions,'rollback_observed':True}))
" > "$evidence_dir/lifecycle-assertions.json"
curl --fail --max-time 10 http://127.0.0.1:8080/api/snapshot > "$evidence_dir/dashboard-snapshot.json"
docker compose cp dashboard:/state/evidence/. "$evidence_dir/"
docker compose cp dashboard:/state/training/holdout-predictions.csv "$evidence_dir/holdout-predictions.csv"
