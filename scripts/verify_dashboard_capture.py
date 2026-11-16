#!/usr/bin/env python3
"""Verify Chrome's actual rendered DOM and screenshot, not just HTTP success."""
from __future__ import annotations

import argparse
from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path
import struct


class ProbeParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.depth = 0
        self.samples: list[str] = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "output" and attributes.get("id") == "ci-render-probe":
            self.active, self.depth = True, 1
        elif self.active:
            self.depth += 1

    def handle_endtag(self, tag):
        if self.active:
            self.depth -= 1
            if self.depth == 0:
                self.active = False

    def handle_data(self, value):
        if self.active:
            self.samples.append(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--scenario", choices=("healthy", "unavailable"), required=True)
    parser.add_argument("--viewport", choices=("desktop", "narrow"), required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    raw_html = args.html.read_text(encoding="utf-8")
    probe = ProbeParser()
    probe.feed(raw_html)
    if len(probe.samples) != 1:
        raise RuntimeError("Rendered browser probe missing or duplicated")
    data = json.loads(probe.samples[0])
    with args.image.open("rb") as image:
        head = image.read(24)
    if len(head) < 24 or head[:8] != b"\x89PNG\r\n\x1a\n":
        raise RuntimeError("Chrome did not produce a valid PNG")
    width, height = struct.unpack(">II", head[16:24])
    if not 200 <= width <= 2500 or not 200 <= height <= 2500 or args.image.stat().st_size < 3000:
        raise AssertionError("Screenshot dimensions/size outside a real rendering")
    if data["overflow"]:
        raise AssertionError("Rendered page overflows the actual viewport")
    if data["externalResourceCount"] or data["executableImageCount"]:
        raise AssertionError("External resources or untrusted HTML execution observed")
    if args.viewport == "desktop" and data["viewport"] < 1100:
        raise AssertionError("Desktop browser rendered a narrow viewport")
    if args.viewport == "narrow" and not 320 <= data["viewport"] <= 760:
        raise AssertionError("Narrow browser missed the responsive breakpoint")
    if args.scenario == "healthy":
        if data["status"] != "healthy · 12s since batch":
            raise AssertionError("Actual synthetic runtime status missing from DOM")
        if data["cardCount"] != 5:
            raise AssertionError("Five dashboard cards did not render")
        if ("MACHINE-001" not in data["alertText"]
                or "feature-visible" not in data["alertText"]
                or "Failure in horizon" not in data["predictionText"]):
            raise AssertionError("Rendered alerts/predictions are missing")
        if '<img src=x onerror="window.__unsafe=1">' not in data["alertText"]:
            raise AssertionError("Untrusted source text was not preserved literally")
    elif data["status"] != "Pipeline unavailable":
        raise AssertionError("Failed snapshot did not produce the real UI outage state")
    report = {
        "schema_version": "industrial-dashboard-browser-1",
        "scenario": args.scenario,
        "viewport_profile": args.viewport,
        "observed_viewport_width": data["viewport"],
        "screenshot_width": width,
        "screenshot_height": height,
        "horizontal_overflow": data["overflow"],
        "rendered_summary_cards": data["cardCount"],
        "rendered_status": data["status"],
        "external_resources": data["externalResourceCount"],
        "untrusted_html_executed_as_markup": bool(data["executableImageCount"]),
        "html_sha256": hashlib.sha256(args.html.read_bytes()).hexdigest(),
        "screenshot_sha256": hashlib.sha256(args.image.read_bytes()).hexdigest(),
        "qualification": "synthetic snapshot rendered by real Chrome; backend integration separate",
        "passed": True,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
