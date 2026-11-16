#!/usr/bin/env bash
# Browser screenshots of the original production HTML with synthetic API data.
set -Eeuo pipefail
umask 077
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
output="${DASHBOARD_BROWSER_EVIDENCE:-evidence/browser}"
if [ -e "$output" ] || [ -L "$output" ]; then
  echo 'Evidence destination exists; refusing overwrite' >&2
  exit 2
fi
mkdir -p "$output"
if [ "$(id -u)" -eq 0 ]; then
  echo 'Browser qualification must run as a non-root user with Chrome sandboxing' >&2
  exit 2
fi
chrome="${DASHBOARD_CHROME_BIN:-$(command -v google-chrome || command -v google-chrome-stable || command -v chromium || true)}"
if [ ! -x "$chrome" ]; then
  echo 'Chrome browser binary missing; cannot qualify rendering' >&2
  exit 2
fi
"$chrome" --version > "$output/browser-version.txt"
git rev-parse HEAD > "$output/source-commit.txt"
fixture_pid=''
stop_fixture() {
  if [ -n "$fixture_pid" ]; then
    kill "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
    fixture_pid=''
  fi
}
trap 'stop_fixture' EXIT
start_fixture() {
  local scenario="$1"
  python3 scripts/dashboard_browser_fixture.py --scenario "$scenario" \
    --port-file "$output/port-$scenario.txt" > "$output/server-$scenario.log" 2>&1 &
  fixture_pid=$!
  for _ in $(seq 1 100); do
    [ -s "$output/port-$scenario.txt" ] && return 0
    if ! kill -0 "$fixture_pid" 2>/dev/null; then
      cat "$output/server-$scenario.log" >&2
      return 1
    fi
    sleep 0.1
  done
  echo 'Browser fixture did not start' >&2
  return 1
}
render() {
  local scenario="$1" profile="$2" size="$3"
  local url="http://127.0.0.1:$(cat "$output/port-$scenario.txt")/"
  local html="$output/$scenario-$profile.html" image="$output/$scenario-$profile.png"
  local profile_dir
  profile_dir="$(mktemp -d "${TMPDIR:-/tmp}/industrial-browser-$scenario-$profile.XXXXXX")"
  timeout --signal=TERM --kill-after=5s 50s "$chrome" \
    --headless=new --disable-gpu --no-first-run --no-default-browser-check \
    --disable-background-networking --disable-extensions --disable-sync \
    --user-data-dir="$profile_dir" --window-size="$size" --force-device-scale-factor=1 \
    --virtual-time-budget=4500 --dump-dom --screenshot="$image" "$url" \
    > "$html" 2> "$output/$scenario-$profile-browser.log"
  python3 scripts/verify_dashboard_capture.py --html "$html" --image "$image" \
    --scenario "$scenario" --viewport "$profile" --report "$output/$scenario-$profile.json"
  # The profile path was created by mktemp immediately above, not user supplied.
  rm -rf -- "$profile_dir"
}
start_fixture healthy
render healthy desktop 1440,1000
render healthy narrow 600,850
stop_fixture
start_fixture unavailable
render unavailable desktop 1440,1000
stop_fixture
python3 - "$output" <<'PY'
import hashlib, json, pathlib, sys
root=pathlib.Path(sys.argv[1])
reports=[json.loads(path.read_text()) for path in sorted(root.glob('*.json'))]
if len(reports)!=3 or not all(report['passed'] for report in reports):
    raise SystemExit('Missing successful browser scenario receipts')
if len({report['screenshot_sha256'] for report in reports})!=3:
    raise SystemExit('Screenshots unexpectedly identical across scenarios')
(root/'summary.json').write_text(json.dumps({
    'status':'passed', 'qualified_scenarios':len(reports),
    'source_sha256':hashlib.sha256(pathlib.Path('src/industrial_mlops/dashboard.py').read_bytes()).hexdigest(),
    'real_browser':True, 'full_service_integration_qualified_by_this_job':False,
},indent=2)+'\n')
PY
