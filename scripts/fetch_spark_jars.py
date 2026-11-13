#!/usr/bin/env python3
"""Fetch or verify the reviewed Spark 3.5.9 Kafka classpath, without JVM/Ivy networking.

Uses standard urllib proxy environment variables. Dependencies and SHA-256 pins are
fixed in config/spark-jars.lock.json; this is deliberately not a Maven resolver.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.request
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
MAVEN = "https://repo.maven.apache.org/maven2/"
COORDINATES = {
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.9",
    "org.apache.kafka:kafka-clients:3.4.1",
    "org.apache.commons:commons-pool2:2.11.1",
}
PARENT = "org.apache.spark:spark-parent_2.12:3.5.9"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_lock(lock: dict) -> None:
    require(lock.get("schema_version") == 1, "Unsupported JAR lock schema")
    require(lock.get("spark_version") == "3.5.9", "Only Spark 3.5.9 is supported")
    require(lock.get("scala_binary_version") == "2.12", "Only Scala 2.12 is supported")
    require(lock.get("repository") == MAVEN, "Only official Maven Central is allowed")
    expected = {(coordinate, suffix) for coordinate in COORDINATES for suffix in ("jar", "pom")}
    expected.add((PARENT, "pom"))
    observed = set()
    for entry in lock["downloads"]:
        coordinate = entry["coordinate"]
        suffix = entry["filename"].rsplit(".", 1)[-1]
        key = (coordinate, suffix)
        require(key in expected and key not in observed, "Unexpected or duplicate download")
        observed.add(key)
        group, artifact, version = coordinate.split(":")
        filename = f"{artifact}-{version}.{suffix}"
        url = f"{MAVEN}{group.replace('.', '/')}/{artifact}/{version}/{filename}"
        require(entry["filename"] == filename and entry["url"] == url, "Invalid Maven path")
        require(0 < entry["bytes"] < 10_000_000, "Unexpected download size")
        for algorithm, length in (("sha256", 64), ("sha512", 128)):
            require(bool(re.fullmatch(f"[a-f0-9]{{{length}}}", entry[algorithm])), "Invalid digest")
        published = entry["published_checksum"]
        algorithm = published["algorithm"]
        require(algorithm in ("sha512", "sha256", "sha1"), "Unsupported sidecar algorithm")
        require(published["url"] == url + "." + algorithm, "Unexpected checksum sidecar")
        require(bool(re.fullmatch(f"[a-f0-9]{{{hashlib.new(algorithm).digest_size * 2}}}",
                                 published["value"])), "Invalid published digest")
    require(observed == expected, "Incomplete Spark Kafka dependency lock")


def checked_bytes(data: bytes, entry: dict) -> None:
    require(len(data) == entry["bytes"], f"Size mismatch: {entry['filename']}")
    published = entry["published_checksum"]
    checks = {"sha256": entry["sha256"], "sha512": entry["sha512"],
              published["algorithm"]: published["value"]}
    for algorithm, expected in checks.items():
        require(hashlib.new(algorithm, data).hexdigest() == expected,
                f"{algorithm} mismatch: {entry['filename']}; existing files are never replaced")


def fetch(url: str, max_bytes: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "industrial-streaming-mlops-jars/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        require(response.geturl().startswith(MAVEN), "Unexpected repository redirect")
        data = response.read(max_bytes + 1)
    require(len(data) <= max_bytes, "Download exceeded the fixed size limit")
    return data


def materialize(entry: dict, output: Path, offline: bool) -> dict:
    directory = output / "poms" if entry["filename"].endswith(".pom") else output
    require(not directory.is_symlink(), "Refusing a symlink cache directory")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / entry["filename"]
    require(not path.is_symlink(), f"Refusing a symlink cache file: {path}")
    if path.exists():
        require(path.is_file() and path.stat().st_size == entry["bytes"], f"Invalid cache file: {path}")
        checked_bytes(path.read_bytes(), entry)
        origin = "verified_locked_cache"
    else:
        require(not offline, f"Offline cache miss: {path}")
        published = entry["published_checksum"]
        sidecar = fetch(published["url"], 1024).decode("ascii").split()[0].lower()
        require(sidecar == published["value"], f"Published checksum changed: {path.name}")
        data = fetch(entry["url"], entry["bytes"])
        checked_bytes(data, entry)
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".download-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
        try:
            temporary.chmod(0o644)
            try:
                os.link(temporary, path)  # Atomic creation; never overwrite a concurrent cache writer.
            except FileExistsError:
                require(not path.is_symlink(), f"Refusing a symlink cache file: {path}")
                checked_bytes(path.read_bytes(), entry)
        finally:
            temporary.unlink(missing_ok=True)
        origin = "downloaded_and_verified_published_checksum"
    return {"coordinate": entry["coordinate"], "path": str(path), "sha256": entry["sha256"],
            "published_checksum_algorithm": entry["published_checksum"]["algorithm"], "origin": origin}


def check_provided(lock: dict, output: Path) -> list[dict]:
    require(importlib.metadata.version("pyspark") == "3.5.9", "Install locked pyspark==3.5.9 first")
    spec = importlib.util.find_spec("pyspark")
    require(spec is not None and spec.origin is not None, "Cannot locate PySpark distribution")
    package = Path(spec.origin).parent.resolve()
    require(output != package and package not in output.parents, "Never write inside the PySpark package")
    jar_directory = package / "jars"
    result = []
    for entry in lock["provided_by_pyspark"]:
        filename = entry["filename"]
        require(Path(filename).name == filename and filename.endswith(".jar"), "Invalid provided JAR path")
        path = jar_directory / filename
        require(path.is_file(), f"Missing pinned PySpark JAR: {filename}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        require(digest == entry["sha256"], f"PySpark bundled JAR differs from reviewed release: {filename}")
        result.append({"coordinate": entry["coordinate"], "path": str(path), "sha256": digest})
    for coordinate in COORDINATES:
        artifact = coordinate.split(":")[1]
        require(not list(jar_directory.glob(artifact + "-*.jar")),
                f"Connector JAR already in PySpark; refusing duplicate classpath: {artifact}")
    return result


def check_poms(lock: dict, output: Path) -> None:
    for review in lock["reviewed_pom_dependencies"]:
        _, artifact, version = review["coordinate"].split(":")
        root = ET.fromstring((output / "poms" / f"{artifact}-{version}.pom").read_bytes())
        dependencies = []
        for element in root.findall("m:dependencies/m:dependency", NS):
            scope = element.findtext("m:scope", "compile", NS)
            if scope == "test":
                continue
            dependencies.append({
                "coordinate": ":".join(element.findtext("m:" + part, "", NS)
                                       for part in ("groupId", "artifactId", "version")),
                "scope": scope,
                "optional": element.findtext("m:optional", "false", NS) == "true",
                "exclusions": [":".join(exclusion.findtext("m:" + part, "", NS)
                                         for part in ("groupId", "artifactId"))
                               for exclusion in element.findall("m:exclusions/m:exclusion", NS)],
            })
        require(dependencies == review["dependencies"], f"POM dependency review differs: {artifact}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=REPO_ROOT / ".tools" / "spark-jars")
    parser.add_argument("--offline", action="store_true", help="Verify cached files without network access")
    parser.add_argument("--print-classpath", action="store_true", help="Print only the four absolute JAR paths as CSV")
    args = parser.parse_args()
    try:
        lock_path = REPO_ROOT / "config" / "spark-jars.lock.json"
        lock_bytes = lock_path.read_bytes()
        lock = json.loads(lock_bytes)
        validate_lock(lock)
        output = args.output.expanduser().resolve()
        require("," not in str(output), "A comma in the output path cannot form a Spark JAR list")
        provided = check_provided(lock, output)
        expected_jars = {entry["filename"] for entry in lock["downloads"] if entry["filename"].endswith(".jar")}
        require(not {path.name for path in output.glob("*.jar")} - expected_jars,
                "Unexpected JARs in cache directory; use a clean dedicated output directory")
        downloads = [materialize(entry, output, args.offline) for entry in lock["downloads"]]
        check_poms(lock, output)
        classpath = ",".join(entry["path"] for entry in downloads if entry["path"].endswith(".jar"))
        receipt = {"schema_version": 1, "spark_version": "3.5.9", "scala_binary_version": "2.12",
                   "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(), "offline": args.offline,
                   "downloads": downloads, "provided_by_pyspark": provided, "classpath": classpath,
                   "qualification": "Files, checksums and reviewed POMs verified; no Kafka or Spark execution implied."}
        receipt_path = output / "verification.json"
        require(not receipt_path.is_symlink(), "Refusing a symlink verification receipt")
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
        print(classpath if args.print_classpath else json.dumps({"receipt": str(receipt_path), "classpath": classpath}))
        return 0
    except (OSError, ValueError, KeyError, IndexError, ET.ParseError, importlib.metadata.PackageNotFoundError) as error:
        print(f"Spark Kafka JAR bootstrap failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
