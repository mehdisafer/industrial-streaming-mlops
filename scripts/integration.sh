#!/usr/bin/env bash
# Bounded real-container smoke on an isolated Compose project.
set -Eeuo pipefail
umask 077
preflight_only=0
if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != --preflight-only ]; }; then
  echo "Usage: bash scripts/integration.sh [--preflight-only]" >&2
  exit 64
fi
if [ "$#" -eq 1 ]; then preflight_only=1; fi
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if ! python_bin="$(command -v python3 || command -v python)"; then
  echo "A Python 3 interpreter is required for preflight and evidence receipts" >&2
  exit 127
fi
run_stamp="$(date -u +%Y%m%dt%H%M%Sz)-$$"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-industrial-smoke-$run_stamp}"
if [[ ! "$COMPOSE_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]{0,95}$ ]]; then
  echo "COMPOSE_PROJECT_NAME must contain 1..96 lowercase letters, digits, '-' or '_'" >&2
  exit 64
fi
evidence_dir="${EVIDENCE_DIR:-evidence/integration/$COMPOSE_PROJECT_NAME}"
if [ -e "$evidence_dir" ] || [ -L "$evidence_dir" ]; then
  echo "Evidence directory already exists; select a new EVIDENCE_DIR: $evidence_dir" >&2
  exit 64
fi
mkdir -p -- "$(dirname -- "$evidence_dir")"
mkdir -- "$evidence_dir" # Exclusive creation also rejects a concurrent evidence writer.
evidence_dir="$(cd -- "$evidence_dir" && pwd)"
industrial_stage=preflight
stack_start_attempted=0
smoke_completed=0
owned_project=0
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
compose() { docker compose --project-name "$COMPOSE_PROJECT_NAME" "$@"; }
record_stage() {
  industrial_stage="$1"
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$industrial_stage" >> "$evidence_dir/stages.log"
}
finish() {
  result_code=$?
  trap - EXIT INT TERM
  set +e
  cleanup_code=-1
  if [ "$owned_project" -eq 1 ] && [ "$stack_start_attempted" -eq 1 ]; then
    # Capture before down: subsequent CI cleanup must not replace these receipts.
    compose ps --all --format json > "$evidence_dir/pre-cleanup-containers.json" 2> "$evidence_dir/capture-errors.log"
    compose logs --no-color > "$evidence_dir/pre-cleanup-container.log" 2>&1
    docker image inspect industrial-streaming-mlops:local apache/kafka:3.9.2 > "$evidence_dir/pre-cleanup-images.json" 2>> "$evidence_dir/capture-errors.log"
    mkdir -p "$evidence_dir/service-evidence"
    compose cp dashboard:/state/evidence/. "$evidence_dir/service-evidence/" >> "$evidence_dir/capture-errors.log" 2>&1
    compose --profile streaming --profile tools down --timeout 45 > "$evidence_dir/cleanup.log" 2>&1
    cleanup_code=$?
    if [ "$cleanup_code" -eq 0 ]; then
      printf '%s\n' "$COMPOSE_PROJECT_NAME" > "$evidence_dir/.cleanup-complete"
    elif [ "$result_code" -eq 0 ]; then
      result_code=1
    fi
  fi
  "$python_bin" - "$evidence_dir/run-receipt.json" "$COMPOSE_PROJECT_NAME" "$industrial_stage" \
    "$result_code" "$cleanup_code" "$started_at" "$owned_project" "$stack_start_attempted" \
    "$smoke_completed" "$preflight_only" <<'PY'
import datetime, hashlib, json, pathlib, subprocess, sys
(output, project, stage, result, cleanup, started, owned, attempted, completed, preflight) = sys.argv[1:]
try:
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, stderr=subprocess.DEVNULL).strip()
except (OSError, subprocess.CalledProcessError):
    commit = None
payload = {
    'schema_version': 1, 'scope': 'isolated container smoke; no Databricks qualification',
    'project': project, 'stage': stage, 'exit_code': int(result), 'git_commit': commit,
    'script_sha256': hashlib.sha256(pathlib.Path('scripts/integration.sh').read_bytes()).hexdigest(),
    'started_at': started, 'finished_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'namespace_ownership_checked': owned == '1', 'stack_start_attempted': attempted == '1',
    'preflight_only': preflight == '1',
    'container_smoke_completed': completed == '1' and result == '0',
    'cleanup_attempted': int(cleanup) >= 0,
    'cleanup_exit_code': int(cleanup) if int(cleanup) >= 0 else None,
    'volumes_preserved': True,
}
pathlib.Path(output).write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
PY
  receipt_code=$?
  if [ "$receipt_code" -ne 0 ] && [ "$result_code" -eq 0 ]; then result_code=1; fi
  printf 'Container smoke exit=%s; project=%s; evidence=%s\n' "$result_code" "$COMPOSE_PROJECT_NAME" "$evidence_dir"
  exit "$result_code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
record_stage preflight
for required in docker curl; do
  if ! command -v "$required" > /dev/null; then
    echo "Missing required command: $required" >&2
    exit 127
  fi
done
docker compose version --short > "$evidence_dir/compose-version.txt"
docker container ls --all --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" \
  --format '{{.ID}}' > "$evidence_dir/existing-containers.txt"
docker volume ls --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" \
  --format '{{.Name}}' > "$evidence_dir/existing-volumes.txt"
docker network ls --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" \
  --format '{{.ID}}' > "$evidence_dir/existing-networks.txt"
if [ -s "$evidence_dir/existing-containers.txt" ] || [ -s "$evidence_dir/existing-volumes.txt" ] \
   || [ -s "$evidence_dir/existing-networks.txt" ]; then
  echo "Compose project already has resources; refusing to reuse, stop or remove them" >&2
  exit 64
fi
"$python_bin" - "$evidence_dir/ports-preflight.json" <<'PY'
import json, pathlib, socket, sys
sockets, failures = [], []
try:
    for port in (5000, 8080, 19092):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets.append(probe)
        try:
            probe.bind(('127.0.0.1', port))
        except OSError as error:
            failures.append({'port': port, 'error': type(error).__name__})
    pathlib.Path(sys.argv[1]).write_text(json.dumps({
        'ports': [5000, 8080, 19092], 'failures': failures,
        'scope': 'loopback bind checks before launch; service bind remains authoritative',
    }, indent=2) + '\n')
finally:
    for probe in sockets:
        probe.close()
if failures:
    print('Required loopback ports are unavailable; existing services were not stopped', file=sys.stderr)
    raise SystemExit(1)
PY
if [ "$preflight_only" -eq 1 ]; then
  record_stage preflight-complete
  exit 0
fi
owned_project=1
printf '%s\n' "$COMPOSE_PROJECT_NAME" > "$evidence_dir/.owns-compose-project"
record_stage build
compose build --pull > "$evidence_dir/build.log" 2>&1
record_stage startup
stack_start_attempted=1
compose up -d --wait --wait-timeout 240 kafka mlflow dashboard
cli() { compose run --rm -T cli "$@"; }
read_json() {
  compose run --rm -T --entrypoint python cli -c "$1"
}
record_stage generate-and-train
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
record_stage initial-stream
cli stream --available-now --duration-seconds 300 --report /state/evidence/stream.json \
  > "$evidence_dir/stream.log" 2>&1
cli verify --output /state/evidence/verification.json > "$evidence_dir/verification.json"
# Failure after an atomic sink commit, before Spark can advance its checkpoint.
record_stage worker-recovery
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
record_stage broker-restart
compose kill -s SIGKILL kafka
compose up -d --wait --wait-timeout 120 kafka
cli stream --available-now --report /state/evidence/broker-restart.json \
  > "$evidence_dir/broker-restart.log" 2>&1
record_stage model-rollback
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
compose cp dashboard:/state/evidence/. "$evidence_dir/"
compose cp dashboard:/state/training/holdout-predictions.csv "$evidence_dir/holdout-predictions.csv"
smoke_completed=1
record_stage complete
