"""Regression checks for the source-only CircleCI security job."""

from pathlib import Path


def test_circleci_security_job_escapes_heredocs() -> None:
    config = Path(".circleci/config.yml").read_text(encoding="utf-8")
    assert "publication-security:" in config
    assert r"python - \<<'PY'" in config
    assert "python - <<'PY'" not in config


def test_circleci_security_job_retains_auditable_source_scope() -> None:
    config = Path(".circleci/config.yml").read_text(encoding="utf-8")
    for required in (
        "--is-shallow-repository",
        "git fetch --tags origin",
        "git rev-list --all --count",
        "scripts/security_inventory.py --scan-history",
        "scripts.repository_archive_inventory",
        "archive-inventory.json",
        "pip-audit --skip-editable",
        "store_artifacts:",
        "path: artifacts/publication-security",
    ):
        assert required in config
