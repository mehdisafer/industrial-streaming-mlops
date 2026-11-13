# syntax=docker/dockerfile:1
# Pin the language patch; record the resolved image digest with each qualification.
FROM python:3.11.18-slim-bookworm

ARG UV_VERSION=0.13.6
ARG SOURCE_REVISION=unrecorded
LABEL org.opencontainers.image.source="https://github.com/mehdisafer/industrial-streaming-mlops" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

ENV PYTHONUNBUFFERED=1 \
    MLFLOW_DISABLE_TELEMETRY=true \
    DO_NOT_TRACK=true \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    JAVA_HOME=/opt/java \
    PATH=/app/.venv/bin:/opt/java/bin:$PATH \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_KAFKA_JARS=/opt/spark-kafka \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    OPENBLAS_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless ca-certificates tini \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir "uv==${UV_VERSION}" \
    && groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid pipeline --create-home pipeline

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY config ./config
COPY schemas ./schemas
COPY scripts ./scripts
RUN uv sync --frozen --no-dev \
    && mkdir -p /state/mlflow/artifacts /state/pipeline /state/checkpoints /state/input /state/training /state/evidence \
    && python scripts/fetch_spark_jars.py --output /opt/spark-kafka \
    && python -c "from pathlib import Path; from pyspark.sql import SparkSession; jars=','.join(str(p) for p in sorted(Path('/opt/spark-kafka').glob('*.jar'))); s=SparkSession.builder.master('local[1]').appName('connector-build-check').config('spark.ui.enabled','false').config('spark.jars',jars).getOrCreate(); assert s._jvm.org.apache.spark.sql.kafka010.KafkaSourceProvider().shortName()=='kafka'; s.stop()" \
    && python -c "import hashlib,json,pathlib,pyspark; jars=list((pathlib.Path(pyspark.__file__).parent/'jars').glob('*.jar'))+list(pathlib.Path('/opt/spark-kafka').glob('*.jar')); pathlib.Path('/app/runtime-jars.json').write_text(json.dumps([{'file':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(jars)],indent=2)+'\n')" \
    && dpkg-query -W > /app/os-packages.tsv \
    && java -version > /app/java-version.txt 2>&1 \
    && chown -R 10001:10001 /state

USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "industrial_mlops"]
CMD ["--help"]
