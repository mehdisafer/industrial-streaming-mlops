"""Real Kafka transport and Spark Structured Streaming entrypoint."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from industrial_mlops.config import PipelineConfig
from industrial_mlops.store import Store, canonical


def produce(
    input_path: str, bootstrap: str, topic: str, *, partitions: int = 3,
    max_events: int = 100000, rate: float = 0, timeout_seconds: int = 60,
) -> dict:
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError, KafkaTimeoutError, NoError, TopicAlreadyExistsError, for_code

    if (not 1 <= partitions <= 32 or not 1 <= max_events <= 1000000 or rate < 0
            or not 1 <= timeout_seconds <= 120):
        raise ValueError("invalid bounded producer configuration")
    timeout_ms, linger_ms = timeout_seconds * 1000, 5
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=timeout_ms)
    try:
        try:
            admin.create_topics([NewTopic(topic, partitions, 1, topic_configs={
                "retention.ms": str(7 * 86400000), "max.message.bytes": "1048576",
            })])
        except TopicAlreadyExistsError:
            pass
        # Creation success can precede topic/leader metadata propagation. The
        # pinned 2.3.2 describe_topics API has no per-call timeout argument;
        # SDK broker-readiness and request waits remain synchronous.
        # Bound retries with a monotonic deadline and never publish a response
        # that arrives after it. Only known transient metadata/leader errors retry.
        readiness_started = time.monotonic()
        deadline = readiness_started + timeout_seconds
        attempts, retry_reasons, last_reason = 0, {}, "metadata_not_requested"
        metadata_error_codes = {3, 5, 6, 9}

        def metadata_error(code):
            if type(code) is not int:
                raise ValueError("topic metadata has an invalid error code")
            error_type = for_code(code)
            if error_type is NoError:
                return None
            if code not in metadata_error_codes or not error_type.retriable:
                raise error_type(f"metadata for topic {topic}")
            return error_type.__name__

        def readiness_timeout():
            return KafkaTimeoutError(
                f"topic {topic} metadata was not ready within {timeout_seconds}s "
                f"after {attempts} attempts; last state: {last_reason}")

        while True:
            if time.monotonic() >= deadline:
                raise readiness_timeout()
            attempts += 1
            reason, leaders = None, {}
            try:
                descriptions = admin.describe_topics([topic])
            except KafkaError as exc:
                if getattr(exc, "errno", None) not in metadata_error_codes or not exc.retriable:
                    raise
                reason = type(exc).__name__
            else:
                if not isinstance(descriptions, list) or any(
                        not isinstance(item, dict) for item in descriptions):
                    raise ValueError("unexpected topic metadata response")
                matches = [item for item in descriptions if item.get("topic") == topic]
                if not matches:
                    reason = "topic_metadata_absent"
                elif len(matches) != 1:
                    raise ValueError("topic metadata contains duplicate topic entries")
                else:
                    description = matches[0]
                    reason = metadata_error(description.get("error_code"))
                    if reason is None:
                        rows = description.get("partitions")
                        if not isinstance(rows, list):
                            raise ValueError("topic metadata has invalid partitions")
                        seen, partition_error_seen = set(), False
                        for row in rows:
                            if not isinstance(row, dict):
                                raise ValueError("topic metadata has an invalid partition entry")
                            error = metadata_error(row.get("error_code"))
                            partition, leader = row.get("partition"), row.get("leader")
                            if (type(partition) is not int or partition < 0 or partition in seen
                                    or type(leader) is not int):
                                raise ValueError("topic metadata has invalid partition IDs or leaders")
                            seen.add(partition)
                            # Inspect every partition so an earlier transient
                            # error cannot hide a later authorization failure.
                            if error is not None:
                                partition_error_seen = True
                                reason = reason or error
                            elif leader < 0:
                                reason = reason or "partition_leader_unassigned"
                            leaders[partition] = leader
                        if not rows:
                            reason = "partition_metadata_absent"
                        elif not partition_error_seen and seen != set(range(partitions)):
                            raise ValueError(
                                "existing topic partition IDs/count differ; "
                                "use a new topic for the experiment")
            observed_at = time.monotonic()
            if observed_at >= deadline:
                last_reason = reason or "ready_response_after_deadline"
                raise readiness_timeout()
            if reason is None:
                topic_readiness = {
                    "attempts": attempts, "elapsed_seconds": observed_at - readiness_started,
                    "partition_ids": sorted(leaders),
                    "leaders": {str(part): leaders[part] for part in sorted(leaders)},
                    "retry_reasons": retry_reasons,
                }
                break
            last_reason = reason
            retry_reasons[reason] = retry_reasons.get(reason, 0) + 1
            time.sleep(min(.1, deadline - observed_at))
    finally:
        admin.close()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap, acks="all", enable_idempotence=True,
        # kafka-python requires delivery_timeout_ms >= request_timeout_ms + linger_ms.
        max_block_ms=timeout_ms, request_timeout_ms=min(30000, timeout_ms - linger_ms),
        delivery_timeout_ms=timeout_ms,
        max_in_flight_requests_per_connection=1, max_request_size=1048576,
        linger_ms=linger_ms,
    )
    futures, offsets, count = [], {}, 0
    started = time.perf_counter()

    def drain():
        for future in futures:
            metadata = future.get(timeout=timeout_seconds)
            key = str(metadata.partition)
            item = offsets.setdefault(key, {"first": metadata.offset, "last": metadata.offset, "count": 0})
            item["first"] = min(item["first"], metadata.offset)
            item["last"] = max(item["last"], metadata.offset)
            item["count"] += 1
        futures.clear()

    try:
        with Path(input_path).open(encoding="utf-8") as handle:
            for line in handle:
                if count >= max_events:
                    raise ValueError("input exceeds --max-events; publication stopped at the explicit bound")
                payload = line.rstrip("\n")
                if not payload:
                    continue
                try:
                    event = json.loads(payload)
                    key = "|".join(str(event.get(k, "invalid")) for k in (
                        "dataset_id", "site_id", "machine_id"))
                except (json.JSONDecodeError, AttributeError):
                    key = "invalid"
                futures.append(producer.send(topic, key=key.encode(), value=payload.encode()))
                count += 1
                if len(futures) >= 256:
                    drain()
                if rate:
                    delay = count / rate - (time.perf_counter() - started)
                    if delay > 0:
                        time.sleep(min(delay, 1.0))
            drain()
        producer.flush(timeout=timeout_seconds)
    finally:
        producer.close(timeout=timeout_seconds)
    elapsed = time.perf_counter() - started
    return {"topic": topic, "partitions": partitions, "topic_readiness": topic_readiness,
            "acknowledged": count, "offsets": offsets,
            "elapsed_seconds": elapsed, "events_per_second": count / max(elapsed, .000001)}


def load_active(store: Store, tracking_uri: str):
    from industrial_mlops.registry import RegistryClient

    deployment = store.deployment()
    if not deployment:
        store.set_runtime("model", {"status": "not_activated", "pending_retained": True})
        return None
    try:
        return RegistryClient(tracking_uri, deployment["model_name"]).load(
            version=deployment["model_version"])
    except Exception as exc:
        store.set_runtime("model_load_error", {
            "exception": type(exc).__name__, "model_version": deployment["model_version"],
        })
        return None


def source_lag(store: Store, bootstrap: str, topic: str) -> dict:
    """Compare broker end offsets to atomically recorded source coordinates."""
    from kafka import KafkaConsumer, TopicPartition
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap, group_id=None, request_timeout_ms=7000,
            session_timeout_ms=6000, api_version_auto_timeout_ms=3000,
        )
        partition_ids = consumer.partitions_for_topic(topic)
        if not partition_ids:
            return {"status": "unavailable", "reason": "topic_metadata_unavailable"}
        partitions = [TopicPartition(topic, part) for part in sorted(partition_ids)]
        # kafka-python 2.3.x takes its bound from request_timeout_ms here.
        ends = consumer.end_offsets(partitions)
        starts = consumer.beginning_offsets(partitions)
        committed = {row["partition_id"]: row["last"] for row in store.db.execute(
            "SELECT partition_id,MAX(offset_id) last FROM raw_records WHERE topic=? GROUP BY partition_id",
            (topic,))}
        rows = [{
            "partition": part.partition, "retained_start": starts[part], "broker_end": ends[part],
            "sink_next": committed.get(part.partition, starts[part] - 1) + 1,
            "behind": max(0, ends[part] - (committed.get(part.partition, starts[part] - 1) + 1)),
        } for part in partitions]
        return {"status": "observed", "topic": topic, "partitions": rows,
                "offsets_behind": sum(row["behind"] for row in rows)}
    except Exception as exc:
        return {"status": "unavailable", "exception": type(exc).__name__}
    finally:
        if consumer is not None:
            consumer.close()


def stream(
    *, bootstrap: str, topic: str, db_path: str, checkpoint: str, tracking_uri: str,
    config: PipelineConfig | None = None, query_id: str = "industrial-v1",
    available_now: bool = False, duration_seconds: int = 300, max_offsets: int = 1000,
    workers: int = 2, failpoint: str | None = None, fail_batch: int = 0,
    fail_marker: str | None = None, on_started=None,
) -> dict:
    """Run a bounded or continuous micro-batch query; finite timeout never means drained."""
    from pyspark.sql import SparkSession, functions as F, types as T

    config = config or PipelineConfig()
    if not 1 <= max_offsets <= config.max_batch_rows or not 1 <= workers <= 8:
        raise ValueError("offset/worker bound exceeds the reference execution profile")
    if not 1 <= duration_seconds <= 7200:
        raise ValueError("duration must be between 1 and 7200 seconds")
    checkpoint_path = Path(checkpoint)
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    # Bind the checkpoint to one sink/config/topic so changing a path cannot silently lose data.
    identity_path = checkpoint_path / "pipeline-binding.json"
    binding = {
        "query_id": query_id, "topic": topic, "bootstrap": bootstrap,
        "db_path": str(Path(db_path).resolve()), "config": config.to_dict(),
    }
    if identity_path.exists():
        if json.loads(identity_path.read_text()) != binding:
            raise ValueError("checkpoint belongs to another sink, source, query or configuration")
    else:
        identity_path.write_text(canonical(binding), encoding="utf-8")
    builder = (
        SparkSession.builder.appName("industrial-streaming-mlops")
        .master(f"local[{workers}]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", str(workers))
    )
    if os.environ.get("SPARK_KAFKA_JARS"):
        jar_directory = Path(os.environ["SPARK_KAFKA_JARS"])
        jars = sorted(jar_directory.glob("*.jar"))
        if not jars:
            raise ValueError("SPARK_KAFKA_JARS directory contains no connector artifacts")
        builder = builder.config("spark.jars", ",".join(str(path.resolve()) for path in jars))
    elif os.environ.get("SPARK_KAFKA_PRELOADED") != "1":
        builder = (
            builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9")
            .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    query = None
    started = time.perf_counter()
    result = {"engine": "Spark Structured Streaming", "spark_version": spark.version,
              "mode": "availableNow" if available_now else "duration",
              "workers": workers, "max_offsets_per_trigger": max_offsets}
    try:
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", max_offsets)
            .option("kafka.request.timeout.ms", "30000")
            .option("kafka.default.api.timeout.ms", "60000").load()
        )
        envelope = T.StructType([
            T.StructField("event_id", T.StringType()), T.StructField("kind", T.StringType()),
            T.StructField("schema_version", T.StringType()), T.StructField("event_time", T.StringType()),
        ])
        transformed = (
            source.select(
                "topic", "partition", "offset",
                F.col("timestamp").cast("double").alias("published_at"),
                F.base64("value").alias("value_base64"),
                F.col("value").isNull().alias("value_is_null"),
                F.col("value").cast("string").alias("value"),
            )
            .withColumn("envelope", F.from_json("value", envelope))
            .withColumn("business_time", F.to_timestamp("envelope.event_time"))
            .withColumn("payload_bytes", F.coalesce(F.length(F.unbase64("value_base64")), F.lit(0)))
        )
        cached_model, cached_version = None, None

        def sink(frame, batch_id):
            nonlocal cached_model, cached_version
            # Count before collect: never accidentally materialize an unbounded driver batch.
            sizes = frame.agg(F.count("*").alias("rows"), F.sum("payload_bytes").alias("bytes")).first()
            if sizes["rows"] > config.max_batch_rows:
                raise ValueError("Spark micro-batch exceeds sink row bound")
            if (sizes["bytes"] or 0) > config.max_batch_bytes:
                raise ValueError("Spark micro-batch exceeds sink byte bound; reduce maxOffsetsPerTrigger")
            rows = [row.asDict(recursive=True) for row in frame.toLocalIterator()]
            with Store(db_path, config) as store:
                active = store.deployment()
                version = (active["model_name"], active["model_version"]) if active else None
                if cached_model is None or version != cached_version:
                    cached_model = load_active(store, tracking_uri)
                    cached_version = version if cached_model is not None else None
                injection = None
                if failpoint and batch_id == fail_batch:
                    marker = Path(fail_marker) if fail_marker else None
                    if marker is None or not marker.exists():
                        injection = failpoint
                        if marker:
                            marker.parent.mkdir(parents=True, exist_ok=True)
                            marker.write_text(failpoint, encoding="utf-8")
                summary = store.process_batch(
                    rows, query_id, int(batch_id), cached_model, failpoint=injection,
                )
                store.set_runtime("spark_envelopes", {
                    "rows": len(rows),
                    "parsed_ids": sum(bool(row["envelope"] and row["envelope"]["event_id"]) for row in rows),
                    "parsed_business_times": sum(row["business_time"] is not None for row in rows),
                    "payload_bytes": sum(row["payload_bytes"] for row in rows),
                })
                if cached_model is not None:
                    from industrial_mlops.observability import monitor_store
                    store.set_runtime("monitoring", monitor_store(store, cached_model))
                print(canonical({"event": "batch_committed", "query_id": query_id,
                                 "batch_id": int(batch_id), **summary}), flush=True)

        writer = transformed.writeStream.foreachBatch(sink).option(
            "checkpointLocation", str(checkpoint_path / "spark")
        ).queryName(query_id)
        writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="5 seconds")
        query = writer.start()
        if on_started is not None:
            on_started(query)
        completed = query.awaitTermination(duration_seconds)
        if query.isActive:
            query.stop()
        result["drained"] = bool(available_now and completed)
        result["query_id"] = str(query.id)
        result["run_id"] = str(query.runId)
        result["progress"] = list(query.recentProgress)
        result["elapsed_seconds"] = time.perf_counter() - started
        with Store(db_path, config) as store:
            # Retry retained features even if a resumed query has no new Kafka offsets.
            if cached_model is None:
                cached_model = load_active(store, tracking_uri)
            store.score_pending(cached_model)
            if cached_model is not None:
                from industrial_mlops.observability import monitor_store
                store.set_runtime("monitoring", monitor_store(store, cached_model))
            result["source_lag"] = source_lag(store, bootstrap, topic)
            store.set_runtime("source_lag", result["source_lag"])
            store.set_runtime("spark", result)
            result["verification"] = store.verify()
        if available_now and not completed:
            raise TimeoutError("availableNow query did not drain before the configured duration")
        return result
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()
