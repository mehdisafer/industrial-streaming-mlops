"""Download original CircleCI artifacts with explicit authority and bounded writes.

This reads the existing job; it never creates a pipeline or changes account state.
Use CIRCLE_TOKEN from the environment. Never commit that token or download receipts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

PROJECT = "circleci/Um5XQQtD4BE9Jcfy4XTzBb/TdzAEfEDkrojS9NSGQ12Uu"
JOB_NUMBER = 13
MANIFEST = Path("docs/evidence/circleci/manifest.json")
SOURCE_RE = re.compile(r"[0-9a-f]{40}\Z")
TRUSTED_ARTIFACT_HOST = ".circle-artifacts.com"
TRUSTED_STORAGE_HOSTS = (
    ".amazonaws.com", ".cloudfront.net", ".storage.googleapis.com",
    ".googleapis.com",
)


def safe_path(name: str) -> PurePosixPath:
    """Keep every vendor-returned pathname strictly beneath a fresh destination."""
    if not isinstance(name, str) or not name or "\\" in name or "\0" in name or ":" in name:
        raise ValueError("invalid artifact pathname")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in (".", "..", "") for part in name.split("/")):
        raise ValueError("unsafe artifact pathname")
    return path


def is_artifact_url(url: str, *, job_id: str, node: int, path: PurePosixPath) -> bool:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if (parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.port not in (None, 443)
            or not (host == "circle-artifacts.com" or host.endswith(TRUSTED_ARTIFACT_HOST))):
        return False
    suffix = f"/job/{job_id}/artifacts/{node}/{path.as_posix()}"
    return unquote(parsed.path).endswith(suffix)


def allowed_redirect(url: str) -> bool:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443):
        return False
    return (host == "circleci.com"
            or host == "circle-artifacts.com" or host.endswith(TRUSTED_ARTIFACT_HOST)
            or host == "storage.googleapis.com"
            or any(host.endswith(suffix) for suffix in TRUSTED_STORAGE_HOSTS))


class RestrictedRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_redirect(newurl):
            raise ValueError("CircleCI supplied an unapproved redirect destination")
        target = super().redirect_request(req, fp, code, msg, headers, newurl)
        if target is not None and urlsplit(req.full_url).hostname != urlsplit(newurl).hostname:
            for key in list(target.headers):
                if key.lower() in {"circle-token", "authorization", "cookie"}:
                    target.remove_header(key)
        return target


def list_artifacts(opener, *, token: str, project: str, number: int) -> list[dict]:
    if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+", project):
        raise ValueError("invalid CircleCI project slug")
    if number <= 0:
        raise ValueError("job number must be positive")
    base = f"https://circleci.com/api/v2/project/{quote(project, safe='/')}/{number}/artifacts"
    artifacts = []
    seen_tokens = set()
    next_token = None
    for _ in range(25):
        url = base if next_token is None else base + "?" + urlencode({"page-token": next_token})
        req = Request(url, headers={"Circle-Token": token, "Accept": "application/json"})
        with opener.open(req, timeout=40) as response:
            data = response.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError("artifact index exceeds 4 MiB")
        response = json.loads(data)
        if not isinstance(response, dict) or not isinstance(response.get("items"), list):
            raise ValueError("CircleCI artifact index is not a valid items list")
        artifacts.extend(response["items"])
        next_token = response.get("next_page_token")
        if not next_token:
            return artifacts
        if not isinstance(next_token, str) or next_token in seen_tokens:
            raise ValueError("invalid or repeating artifact pagination")
        seen_tokens.add(next_token)
    raise ValueError("artifact pagination exceeded the page bound")


def download(opener, *, artifacts: list[dict], dest: Path, token: str, manifest: dict,
             job_number: int, max_file_bytes: int, max_total_bytes: int) -> dict:
    job_id, source = manifest["job_id"], manifest["source_commit"]
    if not SOURCE_RE.fullmatch(source) or not re.fullmatch(r"[0-9a-f-]{36}", job_id):
        raise ValueError("manifest has an invalid job ID or source SHA")
    if max_file_bytes <= 0 or max_total_bytes <= 0:
        raise ValueError("download limits must be positive")
    validated = []
    seen = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise ValueError("invalid artifact index entry")
        path = safe_path(item.get("path"))
        node = item.get("node_index")
        url = item.get("url")
        if type(node) is not int or node < 0 or not isinstance(url, str):
            raise ValueError("artifact has invalid node or URL")
        if not is_artifact_url(url, job_id=job_id, node=node, path=path):
            raise ValueError(f"artifact URL is not source-bound: {path}")
        if str(path) == "download-manifest.json":
            raise ValueError("artifact collides with the intake manifest")
        if str(path) in seen:
            raise ValueError(f"duplicate artifact path: {path}")
        seen.add(str(path))
        validated.append((path, url))
    required = {"ci-host/commit.txt", "integration/run-receipt.json",
                "integration/final-verification.json", "integration/lifecycle-assertions.json",
                "integration/recovery-assertions.json"}
    if not required.issubset(seen):
        raise ValueError("CircleCI listing lacks one or more required original receipts")
    if not validated:
        raise ValueError("CircleCI artifact index is empty")
    if dest.exists() or dest.is_symlink():
        raise ValueError("output directory already exists: refuse overwrite")
    dest.mkdir(parents=True)
    result = {"schema_version": 1, "status": "failed", "scope": "original CircleCI artifact download",
              "job_id": job_id, "source_commit": source, "job_number": job_number,
              "files": [], "errors": [], "original_digests_are_not_yet_independently_pinned": True}
    total = 0
    try:
        for path, url in sorted(validated, key=lambda item: str(item[0])):
            target = dest.joinpath(*path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            hasher = hashlib.sha256()
            size = 0
            try:
                req = Request(url, headers={"Circle-Token": token, "Accept": "application/octet-stream"})
                with opener.open(req, timeout=90) as response, target.open("xb") as output:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        total += len(chunk)
                        if size > max_file_bytes or total > max_total_bytes:
                            raise ValueError("configured download byte limit exceeded")
                        hasher.update(chunk)
                        output.write(chunk)
                result["files"].append({"path": str(path), "bytes": size, "sha256": hasher.hexdigest()})
            except (HTTPError, URLError, OSError, ValueError) as exc:
                if target.is_file():
                    target.unlink()
                result["errors"].append({"path": str(path), "reason": type(exc).__name__,
                                         "http_status": exc.code if isinstance(exc, HTTPError) else None})
                break
        if not result["errors"]:
            try:
                host = (dest / "ci-host" / "commit.txt").read_text(encoding="utf-8").strip()
                receipt = json.loads((dest / "integration" / "run-receipt.json").read_bytes())
                if not isinstance(receipt, dict) or host != source or receipt.get("git_commit") != source:
                    result["errors"].append({"reason": "source_identity_mismatch"})
            except (OSError, ValueError, UnicodeDecodeError):
                result["errors"].append({"reason": "invalid_source_receipt"})
        if not result["errors"]:
            result["status"] = "downloaded_unpinned"
    finally:
        (dest / "download-manifest.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n",
                                                      encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New directory; existing paths are refused")
    parser.add_argument("--manifest", default=MANIFEST, type=Path)
    parser.add_argument("--project-slug", default=PROJECT)
    parser.add_argument("--job-number", type=int, default=JOB_NUMBER)
    parser.add_argument("--max-file-mib", type=int, default=64)
    parser.add_argument("--max-total-mib", type=int, default=512)
    args = parser.parse_args()
    token = os.environ.get("CIRCLE_TOKEN", "")
    if not token.strip():
        parser.error("CIRCLE_TOKEN must be set in the environment; never pass it as an argument")
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        opener = build_opener(RestrictedRedirect())
        artifacts = list_artifacts(opener, token=token, project=args.project_slug,
                                   number=args.job_number)
        output = download(opener, artifacts=artifacts, dest=args.output, token=token,
                          manifest=manifest, job_number=args.job_number,
                          max_file_bytes=args.max_file_mib * 1024 * 1024,
                          max_total_bytes=args.max_total_mib * 1024 * 1024)
    except (OSError, ValueError, KeyError, TypeError, HTTPError, URLError) as exc:
        # Never include request URL, credentials or signed redirect tokens in diagnostics.
        print(f"CircleCI intake failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({"status": output["status"], "files": len(output["files"]),
                      "errors": output["errors"]}, sort_keys=True))
    return 0 if output["status"] == "downloaded_unpinned" else 1


if __name__ == "__main__":
    raise SystemExit(main())
