#!/usr/bin/env python3
"""Serve the original dashboard HTML with bounded synthetic API data.

This is only a real-browser UI test. Real Kafka/Spark/MLflow integration
is qualified separately by scripts/integration.sh.
"""
from __future__ import annotations

import argparse
import ast
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "industrial_mlops" / "dashboard.py"


def dashboard_html() -> str:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    assignments = [
        n.value for n in tree.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "PAGE" for t in n.targets)
    ]
    if len(assignments) != 1 or not isinstance(assignments[0], ast.Constant) or not isinstance(assignments[0].value, str):
        raise RuntimeError("Dashboard PAGE must remain a literal source string")
    page = assignments[0].value
    if page.count("</main></html>") != 1 or 'id="alerts"' not in page:
        raise RuntimeError("Unexpected dashboard structure; refresh browser qualification")
    # Instrument only the isolated synthetic fixture response, not PAGE itself.
    probe = '''<output id="ci-render-probe" hidden></output><script>
(function(){
  const output = document.getElementById('ci-render-probe');
  function sample(){
    const get = id => document.getElementById(id);
    const summary = {
      viewport: innerWidth,
      documentWidth: document.documentElement.scrollWidth,
      overflow: document.documentElement.scrollWidth > innerWidth + 1,
      status: get('status').textContent.trim(),
      cardCount: get('cards').querySelectorAll('.card').length,
      alertText: get('alerts').textContent,
      predictionText: get('predictions').textContent,
      executableImageCount: document.querySelectorAll('#alerts img, #predictions img').length,
      externalResourceCount: performance.getEntriesByType('resource').filter(e => !e.name.startsWith(location.origin)).length,
    };
    const encoded = JSON.stringify(summary);
    if (output.textContent !== encoded) output.textContent = encoded;
  }
  sample(); setInterval(sample, 200);
})();
</script>'''
    return page.replace("</main></html>", probe + "</main></html>")


def snapshot() -> dict:
    return {
        "counts": {"raw": 4380, "accepted": 2164, "rejected": 24, "duplicate": 4,
                   "predictions": 2092, "alerts": 12, "pending_predictions": 0},
        "runtime": {"model": {"value": {"status": "healthy"}},
                    "checkpoint": {"status": "recovered"}},
        "freshness_seconds": 12.0,
        "deployment": {"model_name": "synthetic-reference", "model_version": "4",
                       "mode": "champion"},
        "alerts": [{"site_id": '<img src=x onerror="window.__unsafe=1">',
                    "machine_id": "MACHINE-001", "score": 0.91,
                    "event_time": "2026-11-15T12:00:00Z", "model_version": "4",
                    "mode": "champion", "feature_id": "feature-visible-evidence-00001",
                    "source_event_ids": ["sensor-1", "sensor-2"]}],
        "predictions": [{"machine_id": "MACHINE-001", "event_time": "2026-11-15T12:00:00Z",
                         "score": 0.91, "label": {"target": True}, "inference_ms": 8.7,
                         "mode": "champion"}],
        "rejections": {"late_event": 1},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file", required=True, type=Path)
    parser.add_argument("--scenario", choices=("healthy", "unavailable"), required=True)
    args = parser.parse_args()
    if args.port < 0 or args.port > 65535:
        parser.error("port must be 0..65535")
    html = dashboard_html().encode("utf-8")
    fixture = json.dumps(snapshot(), allow_nan=False).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                code, content_type, body = 200, "text/html; charset=utf-8", html
            elif self.path == "/api/snapshot":
                if args.scenario == "healthy":
                    code, content_type, body = 200, "application/json", fixture
                else:
                    code, content_type, body = 503, "application/json", b'{"error":"synthetic unavailable"}'
            else:
                self.send_error(404)
                return
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *values):
            print("fixture request: " + fmt % values, flush=True)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    args.port_file.write_text(str(server.server_port) + "\n", encoding="utf-8")
    print(json.dumps({"scenario": args.scenario, "local_port": server.server_port,
                      "source": str(SOURCE.relative_to(ROOT))}), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
