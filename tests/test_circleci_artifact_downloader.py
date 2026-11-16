"""Synthetic, offline coverage for authenticated CircleCI artifact intake.

No external API, CircleCI account, secret or original job artifact is accessed.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.request import Request

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.download_circleci_artifacts import (  # noqa: E402
    JOB_NUMBER, PROJECT, RestrictedRedirect, allowed_redirect, download,
    is_artifact_url, list_artifacts, safe_path,
)
from scripts.verify_circleci_artifacts import CHECKS, verify  # noqa: E402

JOB_ID = "424ad872-14fc-4952-ba99-970ee84cf7d7"
SOURCE = "4bcc55ec470a752bac59dfa45f561dad0261d95e"
TOKEN = "synthetic-test-token-not-a-credential"


class FakeOpener:
    def __init__(self, responses: dict[str, bytes]):
        self.responses = responses
        self.requests: list[Request] = []

    def open(self, request: Request, *, timeout: int):
        assert timeout > 0
        self.requests.append(request)
        if request.full_url not in self.responses:
            raise HTTPError(request.full_url, 404, "fake missing artifact", {}, None)
        return io.BytesIO(self.responses[request.full_url])


def vendor_url(path: str, *, job_id: str = JOB_ID) -> str:
    return (f"https://output.circle-artifacts.com/output/job/{job_id}/"
            f"artifacts/0/{path}")


def sample_artifacts() -> tuple[list[dict], dict[str, bytes]]:
    counts = {
        "raw": 100, "accepted": 45, "rejected": 5, "duplicate": 3, "label": 45,
        "control": 2, "events": 45, "labels": 45, "features": 40,
        "predictions": 40, "alerts": 2, "pending_predictions": 0, "pending_events": 0,
    }
    receipts = {
        "ci-host/commit.txt": (SOURCE + "\n").encode(),
        "integration/run-receipt.json": json.dumps({
            "git_commit": SOURCE, "exit_code": 0, "stage": "complete",
            "container_smoke_completed": True, "namespace_ownership_checked": True,
            "stack_start_attempted": True, "cleanup_attempted": True,
            "cleanup_exit_code": 0, "runtime_inventory_capture": "complete",
            "preflight_only": False,
        }).encode(),
        "integration/final-verification.json": json.dumps({
            "valid": True, "checks": dict.fromkeys(CHECKS, True), "counts": counts,
        }).encode(),
        "integration/lifecycle-assertions.json": json.dumps({
            "rollback_observed": True, "real_prediction_versions": {"1": 12, "2": 28},
        }).encode(),
        "integration/recovery-assertions.json": json.dumps({
            "source_reconciliation": True, "after_commit_recovery": True,
            "acknowledged": 80,
        }).encode(),
    }
    items = [{"path": path, "node_index": 0, "url": vendor_url(path)}
             for path in receipts]
    return items, {vendor_url(path): payload for path, payload in receipts.items()}


def sample_manifest() -> dict:
    return {"source_commit": SOURCE, "job_id": JOB_ID, "expected_sha256": {}}


@pytest.mark.parametrize("name", [
    "../secret", "/absolute", "integration/../escape", "integration//double",
    "integration/./dot", r"integration\windows", "C:/drive", "a:\0",
])
def test_unsafe_paths_are_rejected(name):
    with pytest.raises(ValueError):
        safe_path(name)


def test_vendor_url_is_bound_to_host_source_job_and_node():
    path = safe_path("integration/run-receipt.json")
    assert is_artifact_url(vendor_url(str(path)), job_id=JOB_ID, node=0, path=path)
    assert not is_artifact_url(vendor_url(str(path), job_id="a" * 36),
                               job_id=JOB_ID, node=0, path=path)
    assert not is_artifact_url(vendor_url(str(path)).replace("https://", "http://"),
                               job_id=JOB_ID, node=0, path=path)
    assert not is_artifact_url(vendor_url(str(path)).replace(
        "output.circle-artifacts.com", "evil.example"),
        job_id=JOB_ID, node=0, path=path)
    assert not is_artifact_url(vendor_url(str(path)), job_id=JOB_ID, node=2, path=path)


def test_redirect_rejects_untrusted_destination_and_strips_token_off_origin():
    assert not allowed_redirect("http://169.254.169.254/latest/meta-data")
    assert not allowed_redirect("https://evil.example/credential")
    handler = RestrictedRedirect()
    request = Request("https://output.circle-artifacts.com/job/fake",
                      headers={"Circle-Token": TOKEN, "Accept": "application/octet-stream"})
    with pytest.raises(ValueError):
        handler.redirect_request(request, None, 302, "Found", {},
                                 "https://evil.example/credential")
    redirected = handler.redirect_request(request, None, 302, "Found", {},
                                          "https://s3.amazonaws.com/signed-object")
    assert redirected is not None
    assert all(name.lower() != "circle-token" for name in redirected.headers)


def test_paged_listing_preserves_source_and_token_without_implicit_mutation():
    root = f"https://circleci.com/api/v2/project/{PROJECT}/{JOB_NUMBER}/artifacts"
    items, _ = sample_artifacts()
    opener = FakeOpener({
        root: json.dumps({"items": items[:2], "next_page_token": "next"}).encode(),
        root + "?page-token=next": json.dumps({
            "items": items[2:], "next_page_token": None,
        }).encode(),
    })
    listed = list_artifacts(opener, token=TOKEN, project=PROJECT, number=JOB_NUMBER)
    assert listed == items
    assert len(opener.requests) == 2
    assert all(request.get_header("Circle-token") == TOKEN for request in opener.requests)


def test_download_preserves_original_bytes_then_requires_pinned_sha(tmp_path: Path):
    artifacts, payloads = sample_artifacts()
    output = tmp_path / "download"
    result = download(FakeOpener(payloads), artifacts=artifacts, dest=output,
                      token=TOKEN, manifest=sample_manifest(), job_number=JOB_NUMBER,
                      max_file_bytes=100_000, max_total_bytes=500_000)
    assert result["status"] == "downloaded_unpinned"
    assert len(result["files"]) == 5 and not result["errors"]
    assert result["job_number"] == JOB_NUMBER
    assert TOKEN not in (output / "download-manifest.json").read_text()
    for item in result["files"]:
        assert (output / item["path"]).read_bytes() == payloads[vendor_url(item["path"])]
        assert item["sha256"] == hashlib.sha256(payloads[vendor_url(item["path"])]).hexdigest()
    manifest_path = tmp_path / "audit.json"
    manifest_path.write_text(json.dumps(sample_manifest()))
    check = verify(output / "integration", manifest_path)
    assert check["status"] == "observed_unpinned"
    pinned = sample_manifest()
    pinned["expected_sha256"] = {
        item["path"].split("/")[-1]: item["sha256"]
        for item in result["files"] if item["path"].startswith("integration/")
    }
    manifest_path.write_text(json.dumps(pinned))
    assert verify(output / "integration", manifest_path)["status"] == "verified"


@pytest.mark.parametrize("invalid", [
    "../escape", "download-manifest.json", "integration/run-receipt.json",
])
def test_vendor_path_escape_collision_or_duplicate_is_rejected_before_write(tmp_path, invalid):
    items, payloads = sample_artifacts()
    if invalid == "integration/run-receipt.json":
        items.append(items[1].copy())
    else:
        items.append({"path": invalid, "node_index": 0, "url": vendor_url(invalid)})
    output = tmp_path / "download"
    with pytest.raises(ValueError):
        download(FakeOpener(payloads), artifacts=items, dest=output, token=TOKEN,
                 manifest=sample_manifest(), job_number=JOB_NUMBER,
                 max_file_bytes=100_000, max_total_bytes=500_000)
    assert not output.exists()


def test_bounded_download_fails_closed_and_removes_partial_file(tmp_path):
    items, payloads = sample_artifacts()
    output = tmp_path / "download"
    result = download(FakeOpener(payloads), artifacts=items, dest=output,
                      token=TOKEN, manifest=sample_manifest(), job_number=JOB_NUMBER,
                      max_file_bytes=1, max_total_bytes=10)
    assert result["status"] == "failed" and result["errors"]
    assert not (output / result["errors"][0]["path"]).exists()
    assert (output / "download-manifest.json").exists()


def test_original_source_mismatch_is_not_a_success(tmp_path):
    items, payloads = sample_artifacts()
    payloads[vendor_url("ci-host/commit.txt")] = b"f" * 40
    result = download(FakeOpener(payloads), artifacts=items,
                      dest=tmp_path / "download", token=TOKEN,
                      manifest=sample_manifest(), job_number=JOB_NUMBER,
                      max_file_bytes=100_000, max_total_bytes=500_000)
    assert result["status"] == "failed"
    assert result["errors"] == [{"reason": "source_identity_mismatch"}]


def test_invalid_json_receipt_does_not_mark_download_complete(tmp_path):
    items, payloads = sample_artifacts()
    payloads[vendor_url("integration/run-receipt.json")] = b"not json"
    result = download(FakeOpener(payloads), artifacts=items,
                      dest=tmp_path / "download", token=TOKEN,
                      manifest=sample_manifest(), job_number=JOB_NUMBER,
                      max_file_bytes=100_000, max_total_bytes=500_000)
    assert result["status"] == "failed"
    assert result["errors"] == [{"reason": "invalid_source_receipt"}]
