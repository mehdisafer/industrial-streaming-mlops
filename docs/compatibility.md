# Compatibility and component provenance

I keep the local runtime and the Databricks qualification separate. A successful
open-source Spark run says nothing about workspace permissions, vendor runtime
behaviour, or a remote Kafka connection. The evidence report records what ran.

## Selected local versions

| Component | Reference version | Why it is here | Official source |
| --- | --- | --- | --- |
| Python | 3.11.18 in the container | Common Python minor with the provider target; security-maintained 3.11 line | [Python releases](https://www.python.org/downloads/), [official image tags](https://hub.docker.com/_/python) |
| Java | OpenJDK 17, Debian security patch selected at image build | Supported JVM for Spark 3.5 | [Spark runtime requirements](https://spark.apache.org/docs/3.5.9/) |
| Spark / PySpark | 3.5.9 | Patched Spark 3.5 maintenance line; one exact local engine version | [3.5.9 documentation](https://spark.apache.org/docs/3.5.9/), [release announcement](https://lists.apache.org/thread/6j0rsnycnf3ywrjzqwxp4ok0rfzk61cj) |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` | Matches the engine and Scala binary version; resolved into the image at build time | [Kafka integration guide](https://spark.apache.org/docs/3.5.9/structured-streaming-kafka-integration.html) |
| Kafka | `apache/kafka:3.9.2` JVM image | Real KRaft broker with persistent topic data; one broker is sufficient for local recovery exercises | [Official downloads](https://kafka.apache.org/community/downloads/), [KRaft quick start](https://kafka.apache.org/39/getting-started/quickstart/) |
| MLflow client/server | 3.17.2 | HTTP experiment tracking, persistent artifacts, numerical registry versions and aliases | [Release metadata](https://pypi.org/project/mlflow/3.17.2/), [server documentation](https://mlflow.org/docs/latest/self-hosting/architecture/tracking-server/), [registry workflow](https://mlflow.org/docs/latest/ml/model-registry/workflow/) |
| uv | 0.13.6 | Frozen dependency resolution and reproducible environment installation | [Published distribution](https://pypi.org/project/uv/0.13.6/) |

These are selected pins, not a claim that every component is the latest release.
`uv.lock` locks Python dependency resolution. Container tags and Debian package
indexes are not immutable digests: preserve `docker image inspect`, `java
-version`, and the installed package inventory with every container qualification.
The first reference image is not claimed bit-for-bit reproducible across future
Debian security updates. No invented image digest appears in the repository.

The Docker build verifies the four additional connector JARs against the same
fixed manifest as the native setup and loads them from `/opt/spark-kafka`, preserving
PySpark's bundled dependencies. The running stack has an internal Docker network
and does not need Maven access. The native setup uses the fixed
`config/spark-jars.lock.json` and `scripts/fetch_spark_jars.py` to download through
Python's standard proxy handling. The JVM then receives explicit `spark.jars`
paths and does not need Ivy network access. A dependency download failure is a
setup failure, not a passing integration test.

### Native Kafka connector classpath

After installing the frozen Python environment, prepare the classpath with:

```bash
.venv/bin/python scripts/fetch_spark_jars.py
export SPARK_KAFKA_JARS="$PWD/.tools/spark-jars"
# Subsequent verification can prohibit all network access:
.venv/bin/python scripts/fetch_spark_jars.py --offline
```

The application variable is a directory. The separate `--print-classpath` option
prints a comma-separated list for tools that configure Spark's `spark.jars` directly.

Only four additional JARs are downloaded into `.tools/spark-jars/`. The fetcher
checks five locked POMs, their reviewed dependency scopes/exclusions, all downloaded
bytes, and eleven relevant JARs already provided by PySpark. It fails on conflicting
cache bytes or extra JARs and never modifies PySpark's own JAR directory.

| Additional artifact | Version | Official dependency source |
| --- | --- | --- |
| `spark-sql-kafka-0-10_2.12` | 3.5.9 | [Connector POM](https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.pom) |
| `spark-token-provider-kafka-0-10_2.12` | 3.5.9 | [Token provider POM](https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.pom) |
| `kafka-clients` | 3.4.1 | [Kafka client POM](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.pom) |
| `commons-pool2` | 2.11.1 | [Pool POM](https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.pom) |

The [Spark parent POM](https://repo.maven.apache.org/maven2/org/apache/spark/spark-parent_2.12/3.5.9/spark-parent_2.12-3.5.9.pom)
manages Snappy 1.1.10.5, SLF4J 2.0.7, LZ4 1.8.0 and Zstd 1.5.5-4. Those bundled
versions are retained. Both Spark connector dependency edges exclude Kafka's
older Zstd dependency. Optional CGLIB/ASM and test dependencies are omitted.

The lock records actual SHA-256/SHA-512 hashes of the downloaded files and each
published checksum URL. Kafka publishes SHA-512 sidecars. The reviewed Spark and
Pool files returned HTTP 404 for SHA-512 and SHA-256 sidecars; their published
SHA-1 was checked before computing the stronger local lock hashes. SHA-1 and an
observed content hash are not publisher signature verification. The generated
`verification.json` is dependency setup evidence only; a successful fetch does not
establish a Kafka streaming run.

## Semantics that constrain the implementation

- [`foreachBatch`](https://spark.apache.org/docs/3.5.9/structured-streaming-programming-guide.html)
  provides at-least-once writes by default. The local database transaction and
  replay ledger close a specific sink retry window; they do not certify the
  whole system as exactly-once.
- [`maxOffsetsPerTrigger`](https://spark.apache.org/docs/3.5.9/structured-streaming-kafka-integration.html)
  bounds each Kafka micro-batch. Spark's checkpoint and observed source offsets
  drive recovery. A shared manually forced consumer group can interfere with
  another query, so the reference leaves group allocation to Spark.
- A watermark does not produce a quarantine stream automatically. Raw bytes,
  source coordinates and explicit dispositions make rejection and deduplication
  countable. Each machine keeps its partition for the life of a checkpoint.
- MLflow registry metadata needs a database backend. The reference uses a real
  HTTP server, SQLite metadata and a mounted artifact directory. Artifact access
  is proxied with `--artifacts-destination`; clients do not receive container-only
  filesystem paths. The server allowlist includes its internal service hostname.
- Model aliases are resolved to a numerical version before loading; prediction
  metadata records that version. Validation, promotion, worker activation and
  rollback have different outcomes and evidence.

## Databricks target

The prepared provider target is an **existing authorized Databricks Runtime 15.4
LTS** compute. Its [release notes](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts)
list Spark 3.5.0 and Python 3.11.11. Databricks adds and backports changes; matching
the Spark minor family does not establish byte-level or behavioural equivalence.

The bundle does not install PySpark or the OSS Kafka connector over the vendor
runtime. It synchronizes the shared Python source and uses Databricks' native
[Kafka source](https://docs.databricks.com/aws/en/connect/streaming/kafka/) and Delta.
MLflow, scikit-learn, NumPy, pandas and skops are pinned as task libraries to match the
model. **Their overlay on DBR's preinstalled libraries has not been executed or
qualified without an authorized workspace.** Record actual resolved versions
and test them before accepting this provider path; do not assume the local lock
is a Databricks environment lock.

Models use MLflow's skops serializer (`model.skops`), not pickle. Both loaders
require the expected artifact filename, digest and exact serializer/type policy;
the only extra trusted type for the baseline is the project-owned
`industrial_mlops.modeling.BaselineRiskModel`. A model cannot add an arbitrary
bundled code path or expand its own trusted-type list. Logged-model artifact
resolution uses an explicit MLflow client and checks the source run identity.
These controls do not turn a content digest into a security certification.

The provider path is a bounded driver replay of Delta bronze, using the same
causal feature code. It is suitable for a repeatable qualification dataset, not
evidence of distributed feature processing. See [the provider procedure](databricks.md).

## CI action provenance

The workflow uses full commits verified from the action owners' release pages:

| Action | Release | Commit |
| --- | --- | --- |
| `actions/checkout` | [v4.4.0](https://github.com/actions/checkout/releases/tag/v4.4.0) | `11d5960a326750d5838078e36cf38b85af677262` |
| `actions/upload-artifact` | [v4.6.2](https://github.com/actions/upload-artifact/releases/tag/v4.6.2) | `ea165f8d65b6e75b540449e92b4886f43607fa02` |

CI uses read-only repository permissions and does not deploy Databricks resources.
Unit checks run on every change. Real service integration is selected for source,
test, configuration, dependency and infrastructure changes, or manual dispatch.
Documentation-only changes do not trigger the heavier service exercise.

## Third-party rights and inventory

Kafka and Spark are Apache projects; the [Apache licensing policy](https://www.apache.org/licenses/LICENSE-2.0.html)
applies to their own code, while bundled components retain their notices. MLflow's
[published metadata](https://pypi.org/project/mlflow/3.17.2/) declares its Apache
license. Container images, the JVM, Python packages and the Databricks service
have their own terms; they are not relicensed by this repository.

`scripts/security_inventory.py` records installed Python distributions, versions
and declared license metadata, and checks common credential patterns in tracked
files and reachable Git file blobs. Unknown metadata remains visible for review.
`pip-audit` checks published dependency advisories independently. These controls
have stated limits and are not certifications. The repository license itself
requires the owner's explicit decision; this work does not select or replace it.
