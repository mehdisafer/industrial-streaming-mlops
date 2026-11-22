"""Select integration scope from the complete event range, retaining the decision."""

import json
import os
from pathlib import Path
import re
import subprocess


EXACT = {"pyproject.toml", "uv.lock", "compose.yaml", "databricks.yml", ".dockerignore"}
PREFIXES = (
    "src/", "tests/", "config/", "schemas/", "scripts/", "infra/", "databricks/",
    "resources/", ".github/workflows/",
)


def available_commit(value):
    return (
        isinstance(value, str)
        and re.fullmatch(r"[0-9a-f]{40}", value) is not None
        and set(value) != {"0"}
        and subprocess.run(
            ["git", "cat-file", "-e", value + "^{commit}"],
            capture_output=True, timeout=30, check=False,
        ).returncode == 0
    )


def select_scope():
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    report = {"integration": True, "selection": "unavailable-event-full-qualification",
              "event_name": event_name, "changed_paths": []}
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return report
    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {**report, "selection": "invalid-event-full-qualification"}
    if not isinstance(event, dict):
        return {**report, "selection": "invalid-event-full-qualification"}
    if event_name == "workflow_dispatch":
        return {**report, "selection": "manual-full-qualification"}
    if event_name == "pull_request":
        # synchronize.before is only the previous PR head, not the PR base.
        # A final documentation push must not hide earlier runtime changes.
        pr = event.get("pull_request") or {}
        if not isinstance(pr, dict):
            return {**report, "selection": "invalid-event-full-qualification"}
        base_ref, head_ref = pr.get("base") or {}, pr.get("head") or {}
        if not isinstance(base_ref, dict) or not isinstance(head_ref, dict):
            return {**report, "selection": "invalid-event-full-qualification"}
        base, head = base_ref.get("sha", ""), head_ref.get("sha", "")
        selection = "full-pr-diff"
        if event.get("action") == "synchronize" and event.get("after") not in (None, "", head):
            return {**report, "selection": "inconsistent-pr-head-full-qualification"}
    elif event_name == "push":
        base, head = event.get("before", ""), event.get("after") or os.environ.get("GITHUB_SHA", "")
        selection = "branch-push-diff"
    else:
        return {**report, "selection": "unsupported-event-full-qualification"}
    try:
        if not available_commit(base) or not available_commit(head):
            return {**report, "selection": "unavailable-reference-full-qualification"}
        changed = subprocess.check_output(
            ["git", "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z",
             base, head, "--"], timeout=30,
        ).decode("utf-8", "surrogateescape").split("\0")
    except (OSError, subprocess.SubprocessError):
        return {**report, "selection": "unavailable-diff-full-qualification"}
    paths = [path for path in changed if path]
    return {**report, "integration": any(path in EXACT or path.startswith(PREFIXES) for path in paths),
            "selection": selection, "base_sha": base, "head_sha": head, "changed_paths": paths}


def main():
    report = select_scope()
    report["checkout_sha"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, timeout=30,
    ).strip()
    output = json.dumps(report, indent=2) + "\n"
    destination = Path("evidence/ci/scope.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(output, encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
        handle.write("integration=" + str(report["integration"]).lower() + "\n")
    print(output, end="")


if __name__ == "__main__":
    main()
