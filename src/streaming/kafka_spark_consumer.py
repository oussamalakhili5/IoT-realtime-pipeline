
"""Consume, validate, and analyze IoT events with Spark Structured Streaming."""

import logging
import sys
from collections import Counter
from pathlib import Path
import os
from dotenv import load_dotenv

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_trunc,
    from_json,
    length,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    stddev_pop,
    to_date,
    to_timestamp,
    trim,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Project imports and configuration
# ---------------------------------------------------------------------------

# Add the project root to Python's import path.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

    # Import project modules only after configuring the path.
from src.storage.postgres_writer import (
    persist_event_summary_batch,
    persist_sensor_events_partition,
    persist_window_metrics_partition,
    persist_window_summary_batch,
)
from src.streaming.kafka_consumer_config import (
    resolve_consumer_config,
    silo_event_batch_path,
    silo_window_batch_path,
    silo_window_run_batch_path,
)
from src.streaming.json_payload import normalize_json_payload_column


from src.streaming.anomaly_rules import (
    MAX_HUMIDITY_RANGE_PCT,
    MAX_PRESSURE_RANGE_PA,
    MAX_TEMPERATURE_RANGE_C,
    MIN_OBSERVATIONS_PER_METRIC,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# KAFKA_TOPIC = "iot-sensor-events-march-geospatial-test"
# KAFKA_TOPIC = "iot-sensor-events-march-pipeline"
WINDOW_DURATION = "10 minutes"
WINDOW_SLIDE = "5 minutes"
WATERMARK_DELAY = "10 minutes"

PROCESSING_INTERVAL = "5 seconds"

CONSUMER_CONFIG = resolve_consumer_config(PROJECT_ROOT)
KAFKA_TOPIC = CONSUMER_CONFIG.topic
MAX_OFFSETS_PER_TRIGGER = CONSUMER_CONFIG.max_offsets_per_trigger
EVENT_PERSISTENCE_MODE = CONSUMER_CONFIG.event_persistence_mode
EVENT_SILO_RUN_ID = CONSUMER_CONFIG.event_silo_run_id
WINDOW_PERSISTENCE_MODE = CONSUMER_CONFIG.window_persistence_mode
CHECKPOINT_DIR = CONSUMER_CONFIG.routing_checkpoint_dir
WINDOW_CHECKPOINT_DIR = CONSUMER_CONFIG.window_checkpoint_dir


def _refresh_consumer_config_from_environment():
    """Apply configuration after dotenv files have been loaded."""
    global CONSUMER_CONFIG, KAFKA_TOPIC, MAX_OFFSETS_PER_TRIGGER
    global EVENT_PERSISTENCE_MODE, EVENT_SILO_RUN_ID
    global WINDOW_PERSISTENCE_MODE
    global CHECKPOINT_DIR
    global WINDOW_CHECKPOINT_DIR

    CONSUMER_CONFIG = resolve_consumer_config(PROJECT_ROOT)
    KAFKA_TOPIC = CONSUMER_CONFIG.topic
    MAX_OFFSETS_PER_TRIGGER = CONSUMER_CONFIG.max_offsets_per_trigger
    EVENT_PERSISTENCE_MODE = CONSUMER_CONFIG.event_persistence_mode
    EVENT_SILO_RUN_ID = CONSUMER_CONFIG.event_silo_run_id
    WINDOW_PERSISTENCE_MODE = CONSUMER_CONFIG.window_persistence_mode
    CHECKPOINT_DIR = CONSUMER_CONFIG.routing_checkpoint_dir
    WINDOW_CHECKPOINT_DIR = CONSUMER_CONFIG.window_checkpoint_dir


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical IoT event schema
# ---------------------------------------------------------------------------

EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField(
            "location",
            StructType(
                [
                    StructField("location_id", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "measurements",
            StructType(
                [
                    StructField("pressure_raw", StringType(), True),
                    StructField("pressure_numeric", DoubleType(), True),
                    StructField("pressure_pa_candidate", DoubleType(), True),
                    StructField("pressure_unit_candidate", StringType(), True),
                    StructField("temperature_c", DoubleType(), True),
                    StructField("humidity_pct", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "quality",
            StructType(
                [
                    StructField("pressure_status", StringType(), True),
                    StructField("temperature_status", StringType(), True),
                    StructField("humidity_status", StringType(), True),
                    StructField("humidity_at_boundary", BooleanType(), True),
                    StructField("record_status", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

KAFKA_JSON_CORRUPT_RECORD_FIELD = "_iot_corrupt_record"
KAFKA_JSON_PARSE_SCHEMA = StructType(
    EVENT_SCHEMA.fields
    + [StructField(KAFKA_JSON_CORRUPT_RECORD_FIELD, StringType(), True)]
)


# ---------------------------------------------------------------------------
# Kafka JSON parsing and processing status
# ---------------------------------------------------------------------------

def parse_and_classify_kafka_messages(kafka_stream):
    """Decode Kafka values, validate their event fields, and classify them."""
    parsed_stream = (
        kafka_stream
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("json_payload"),
        )
        .withColumn(
            "json_payload",
            normalize_json_payload_column(col("json_payload")),
        )
        .withColumn(
            "event",
            from_json(
                col("json_payload"),
                KAFKA_JSON_PARSE_SCHEMA,
                {"columnNameOfCorruptRecord": KAFKA_JSON_CORRUPT_RECORD_FIELD},
            ),
        )
    )

    return (
        parsed_stream
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("json_payload").alias("event_json"),
            (
                col("event").isNotNull()
                & col(f"event.{KAFKA_JSON_CORRUPT_RECORD_FIELD}").isNull()
            ).alias("json_parsed"),
            col("event.schema_version").alias("schema_version"),
            col("event.sensor_id").alias("sensor_id"),
            col("event.timestamp").alias("event_timestamp_raw"),
            col("event.location.location_id").alias("location_id"),
            col("event.location.latitude").alias("latitude"),
            col("event.location.longitude").alias("longitude"),
            col("event.measurements.pressure_raw").alias("pressure_raw"),
            col("event.measurements.pressure_numeric").alias(
                "pressure_numeric"
            ),
            col("event.measurements.pressure_pa_candidate").alias(
                "pressure_pa_candidate"
            ),
            col("event.measurements.pressure_unit_candidate").alias(
                "pressure_unit_candidate"
            ),
            col("event.measurements.temperature_c").alias(
                "temperature_c"
            ),
            col("event.measurements.humidity_pct").alias("humidity_pct"),
            col("event.quality.pressure_status").alias("pressure_status"),
            col("event.quality.temperature_status").alias(
                "temperature_status"
            ),
            col("event.quality.humidity_status").alias("humidity_status"),
            col("event.quality.humidity_at_boundary").alias(
                "humidity_at_boundary"
            ),
            col("event.quality.record_status").alias("record_status"),
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp_raw")),
        )
        .withColumn(
            "timestamp_valid",
            col("event_timestamp").isNotNull(),
        )
        .withColumn(
            "processing_status",
            when(
                ~col("json_parsed"),
                lit("invalid_json"),
            )
            .when(
                col("schema_version").isNull()
                | (~col("schema_version").isin("1.0", "1.1")),
                lit("unsupported_schema"),
            )
            .when(
                col("sensor_id").isNull()
                | (length(trim(col("sensor_id"))) == 0),
                lit("invalid_sensor_id"),
            )
            .when(
                col("event_timestamp").isNull(),
                lit("invalid_timestamp"),
            )
            .when(
                col("record_status") == "review_required",
                lit("review_required"),
            )
            .when(
                col("record_status").isin(
                    "accepted_candidate",
                    "accepted_with_humidity_boundary",
                ),
                lit("accepted"),
            )
            .otherwise(lit("unknown_quality_status")),
        )
    )


def build_windowed_metrics(events):
    """Aggregate accepted events using the configured sliding windows."""
    accepted_events = events.filter(
        (col("processing_status") == "accepted")
        & col("event_timestamp").isNotNull()
    )

    return (
        accepted_events
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            window(
                col("event_timestamp"),
                WINDOW_DURATION,
                WINDOW_SLIDE,
            ),
            col("topic"),
            col("sensor_id"),
            col("location_id"),
            col("latitude"),
            col("longitude"),
        )
        .agg(
            count("*").alias("measurement_count"),
            count("pressure_pa_candidate").alias(
                "pressure_observation_count"
            ),
            count("temperature_c").alias(
                "temperature_observation_count"
            ),
            count("humidity_pct").alias(
                "humidity_observation_count"
            ),
            avg("pressure_pa_candidate").alias("avg_pressure_pa"),
            spark_min("pressure_pa_candidate").alias("min_pressure_pa"),
            spark_max("pressure_pa_candidate").alias("max_pressure_pa"),
            stddev_pop("pressure_pa_candidate").alias(
                "stddev_pressure_pa"
            ),
            avg("temperature_c").alias("avg_temperature_c"),
            spark_min("temperature_c").alias("min_temperature_c"),
            spark_max("temperature_c").alias("max_temperature_c"),
            stddev_pop("temperature_c").alias("stddev_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            spark_min("humidity_pct").alias("min_humidity_pct"),
            spark_max("humidity_pct").alias("max_humidity_pct"),
            stddev_pop("humidity_pct").alias("stddev_humidity_pct"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("topic"),
            col("sensor_id"),
            col("location_id"),
            col("latitude"),
            col("longitude"),
            col("measurement_count"),
            col("avg_pressure_pa"),
            col("min_pressure_pa"),
            col("max_pressure_pa"),
            col("stddev_pressure_pa"),
            col("avg_temperature_c"),
            col("min_temperature_c"),
            col("max_temperature_c"),
            col("stddev_temperature_c"),
            col("avg_humidity_pct"),
            col("min_humidity_pct"),
            col("max_humidity_pct"),
            col("stddev_humidity_pct"),
            col("pressure_observation_count"),
            col("temperature_observation_count"),
            col("humidity_observation_count"),
        )
        .withColumn(
            "pressure_range_pa",
            col("max_pressure_pa") - col("min_pressure_pa"),
        )
        .withColumn(
            "temperature_range_c",
            col("max_temperature_c") - col("min_temperature_c"),
        )
        .withColumn(
            "humidity_range_pct",
            col("max_humidity_pct") - col("min_humidity_pct"),
        )
        .withColumn(
            "pressure_variation_anomaly",
            col("pressure_range_pa") > MAX_PRESSURE_RANGE_PA,
        )
        .withColumn(
            "temperature_variation_anomaly",
            col("temperature_range_c") > MAX_TEMPERATURE_RANGE_C,
        )
        .withColumn(
            "humidity_variation_anomaly",
            col("humidity_range_pct") > MAX_HUMIDITY_RANGE_PCT,
        )
        .withColumn(
            "anomaly_status",
            when(
                (col("pressure_observation_count")
                 < MIN_OBSERVATIONS_PER_METRIC)
                | (col("temperature_observation_count")
                   < MIN_OBSERVATIONS_PER_METRIC)
                | (col("humidity_observation_count")
                   < MIN_OBSERVATIONS_PER_METRIC),
                lit("insufficient_data"),
            )
            .when(
                col("pressure_variation_anomaly")
                | col("temperature_variation_anomaly")
                | col("humidity_variation_anomaly"),
                lit("anomaly_suspected"),
            )
            .otherwise(lit("normal")),
        )
    )


# ---------------------------------------------------------------------------
# Event routing
# ---------------------------------------------------------------------------

def build_event_summary_frames(events):
    """Summarize accepted source events at global-hour and sensor-day grains."""
    accepted = events.filter(
        (col("processing_status") == "accepted")
        & col("event_timestamp").isNotNull()
    )
    metric_aggregates = [
        count("*").alias("accepted_event_count"),
        count("pressure_pa_candidate").alias("pressure_observation_count"),
        spark_sum("pressure_pa_candidate").alias("pressure_sum"),
        spark_min("pressure_pa_candidate").alias("pressure_min"),
        spark_max("pressure_pa_candidate").alias("pressure_max"),
        count("temperature_c").alias("temperature_observation_count"),
        spark_sum("temperature_c").alias("temperature_sum"),
        spark_min("temperature_c").alias("temperature_min"),
        spark_max("temperature_c").alias("temperature_max"),
        count("humidity_pct").alias("humidity_observation_count"),
        spark_sum("humidity_pct").alias("humidity_sum"),
        spark_min("humidity_pct").alias("humidity_min"),
        spark_max("humidity_pct").alias("humidity_max"),
    ]

    hourly = (
        accepted.groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("topic"),
        )
        .agg(*metric_aggregates)
        .select(
            col("topic"),
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            *[
                col(name)
                for name in (
                    "accepted_event_count",
                    "pressure_observation_count",
                    "pressure_sum",
                    "pressure_min",
                    "pressure_max",
                    "temperature_observation_count",
                    "temperature_sum",
                    "temperature_min",
                    "temperature_max",
                    "humidity_observation_count",
                    "humidity_sum",
                    "humidity_min",
                    "humidity_max",
                )
            ],
        )
    )

    daily_sensor = (
        accepted.groupBy(
            to_date(col("event_timestamp")).alias("summary_date"),
            col("topic"),
            col("sensor_id"),
            col("location_id"),
            col("latitude"),
            col("longitude"),
        )
        .agg(*metric_aggregates)
    )
    return hourly, daily_sensor


def _collect_window_status_summary_rows(batch_df):
    """Aggregate window statuses in Spark and collect only hourly counts."""
    known_statuses = (
        "normal",
        "anomaly_suspected",
        "insufficient_data",
    )
    status_frame = batch_df.select(
        date_trunc("hour", col("window_start")).alias("hour_start"),
        when(
            col("anomaly_status").isin(*known_statuses),
            col("anomaly_status"),
        )
        .otherwise(lit("other"))
        .alias("_summary_status"),
    )
    hourly_counts = (
        status_frame.groupBy("hour_start")
        .agg(
            spark_sum(
                when(col("_summary_status") == "normal", lit(1))
                .otherwise(lit(0))
            ).alias("normal_count"),
            spark_sum(
                when(
                    col("_summary_status") == "anomaly_suspected",
                    lit(1),
                )
                .otherwise(lit(0))
            ).alias("anomaly_suspected_count"),
            spark_sum(
                when(
                    col("_summary_status") == "insufficient_data",
                    lit(1),
                )
                .otherwise(lit(0))
            ).alias("insufficient_data_count"),
            spark_sum(
                when(col("_summary_status") == "other", lit(1))
                .otherwise(lit(0))
            ).alias("other_count"),
        )
        .orderBy("hour_start")
    )
    return [row.asDict(recursive=True) for row in hourly_counts.collect()]


def _write_silo_batch(batch_df, path):
    """Replace only the deterministic output prefix for this batch retry."""
    batch_df.write.mode("overwrite").parquet(path)


def _write_silo_event_batch(batch_df, path):
    """Archive classified events with a queryable status partition."""
    (
        batch_df.write
        .mode("overwrite")
        .partitionBy("processing_status")
        .parquet(path)
    )


def route_batch(batch_df, batch_id):
    """Route accepted events according to the configured persistence mode."""
    if EVENT_PERSISTENCE_MODE == "postgres":
        batch_rows = batch_df.collect()
        status_counts = Counter(
            row.processing_status for row in batch_rows
        )
        accepted_events = [
            row for row in batch_rows
            if row.processing_status == "accepted"
        ]
    else:
        status_rows = (
            batch_df.groupBy("processing_status")
            .count()
            .collect()
        )
        status_counts = Counter(
            {
                row["processing_status"]: row["count"]
                for row in status_rows
            }
        )

    total_count = sum(status_counts.values())
    accepted_count = status_counts.get("accepted", 0)

    logger.info(
        "Routing micro-batch %s | total=%d accepted=%d nonaccepted=%d "
        "statuses=%s",
        batch_id,
        total_count,
        accepted_count,
        total_count - accepted_count,
        dict(status_counts),
    )

    if EVENT_PERSISTENCE_MODE == "postgres":
        persist_sensor_events_partition(accepted_events)
        logger.info(
            "Accepted events persisted to PostgreSQL for micro-batch %s",
            batch_id,
        )

    if EVENT_PERSISTENCE_MODE == "silo" and total_count:
        silo_path = silo_event_batch_path(
            KAFKA_TOPIC,
            EVENT_SILO_RUN_ID,
            batch_id,
        )
        _write_silo_event_batch(batch_df, silo_path)
        logger.info(
            "Classified event Parquet persisted to Silo. "
            "Micro-batch=%s rows=%s accepted=%s path=%s",
            batch_id,
            total_count,
            accepted_count,
            silo_path,
        )

    if WINDOW_PERSISTENCE_MODE == "silo_summaries":
        hourly_summaries, daily_sensor_summaries = build_event_summary_frames(
            batch_df
        )
        persisted = persist_event_summary_batch(
            KAFKA_TOPIC,
            EVENT_SILO_RUN_ID,
            batch_id,
            hourly_summaries.collect(),
            daily_sensor_summaries.collect(),
        )
        logger.info(
            "Compact accepted-event PostgreSQL summaries %s for batch %s",
            "committed" if persisted else "already committed",
            batch_id,
        )



def persist_window_metrics_batch(batch_df, batch_id):
    """Persist detailed windows and the configured PostgreSQL outputs."""

    logger.info(
        "Processing window metrics for micro-batch %s",
        batch_id,
    )

    if WINDOW_PERSISTENCE_MODE == "postgres":
        # Preserve historical behavior in the default mode.
        window_rows = batch_df.collect()
        persist_window_metrics_partition(window_rows)
        if not window_rows:
            logger.info(
                "No window metrics to write for micro-batch %s",
                batch_id,
            )
            return

        # Preserve the established topic/batch path in the default mode.
        silo_path = silo_window_batch_path(KAFKA_TOPIC, batch_id)
        # Overwriting this batch-specific path makes retries idempotent.
        _write_silo_batch(batch_df, silo_path)
        logger.info(
            "Window metrics persisted to PostgreSQL and Silo. "
            "Micro-batch=%s, rows=%s, path=%s",
            batch_id,
            len(window_rows),
            silo_path,
        )
        return

    # Strategy B consumes the batch for the hourly aggregate, anomaly detail,
    # and Parquet output. Cache computed rows across those actions and release
    # the executor storage in all cases.
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        status_rows = _collect_window_status_summary_rows(batch_df)
        if not status_rows:
            # Preserve the PostgreSQL batch-progress claim for empty batches.
            persist_window_summary_batch(
                KAFKA_TOPIC,
                EVENT_SILO_RUN_ID,
                batch_id,
                [],
                [],
            )
            logger.info(
                "No window metrics to write for micro-batch %s",
                batch_id,
            )
            return

        anomaly_rows = (
            batch_df
            .filter(col("anomaly_status") == "anomaly_suspected")
            .collect()
        )
        silo_path = silo_window_run_batch_path(
            KAFKA_TOPIC,
            EVENT_SILO_RUN_ID,
            batch_id,
        )

        # Retries replace only this deterministic topic/run/batch prefix.
        _write_silo_batch(batch_df, silo_path)
        persisted = persist_window_summary_batch(
            KAFKA_TOPIC,
            EVENT_SILO_RUN_ID,
            batch_id,
            status_rows,
            anomaly_rows,
        )
    finally:
        batch_df.unpersist()

    window_count = sum(
        row["normal_count"]
        + row["anomaly_suspected_count"]
        + row["insufficient_data_count"]
        + row["other_count"]
        for row in status_rows
    )
    logger.info(
        "Detailed window metrics persisted to Silo; compact status and "
        "anomaly outputs %s for batch %s. Rows=%s, path=%s",
        "committed" if persisted else "already committed",
        batch_id,
        window_count,
        silo_path,
    )


def main():
    """Start Kafka ingestion, event routing, and windowed analysis."""

    # Load Silo credentials.
    load_dotenv(PROJECT_ROOT / ".env.docker")
    _refresh_consumer_config_from_environment()

    minio_access_key = os.getenv("MINIO_ROOT_USER")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not minio_access_key or not minio_secret_key:
        raise RuntimeError(
            "Missing MinIO credentials in .env.docker."
        )

    # Configure Spark.
    spark = (
        SparkSession.builder
        .appName("IoT-Real-Time-Kafka-Consumer")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Keep the rest of your existing main() function below.


    try:
        logger.info("Starting Spark Structured Streaming.")
        logger.info("Kafka server: %s", KAFKA_BOOTSTRAP_SERVERS)
        logger.info("Kafka topic: %s", KAFKA_TOPIC)
        logger.info(
            "Window configuration: duration=%s, slide=%s, watermark=%s",
            WINDOW_DURATION,
            WINDOW_SLIDE,
            WATERMARK_DELAY,
        )
        logger.info(
            "Anomaly thresholds: pressure=%s Pa, temperature=%s C, "
            "humidity=%s pct, minimum observations=%s",
            MAX_PRESSURE_RANGE_PA,
            MAX_TEMPERATURE_RANGE_C,
            MAX_HUMIDITY_RANGE_PCT,
            MIN_OBSERVATIONS_PER_METRIC,
        )
        logger.info(
            "Maximum Kafka offsets per trigger: %d",
            MAX_OFFSETS_PER_TRIGGER,
        )
        logger.info(
            "Event persistence mode: %s | window persistence mode: %s",
            EVENT_PERSISTENCE_MODE,
            WINDOW_PERSISTENCE_MODE,
        )

        # -------------------------------------------------------------------
        # 1. Read events from Kafka
        # -------------------------------------------------------------------

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option(
                "maxOffsetsPerTrigger",
                str(MAX_OFFSETS_PER_TRIGGER),
            )
            .load()
        )

        # -------------------------------------------------------------------
        # 2. Parse JSON using the canonical schema
        # -------------------------------------------------------------------

        events = parse_and_classify_kafka_messages(kafka_stream)

        # -------------------------------------------------------------------
        # 4. Calculate sliding-window statistics
        # -------------------------------------------------------------------

        windowed_metrics = build_windowed_metrics(events)

        # -------------------------------------------------------------------
        # 6. Prepare checkpoint directories
        # -------------------------------------------------------------------

        CHECKPOINT_DIR.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        WINDOW_CHECKPOINT_DIR.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # -------------------------------------------------------------------
        # 7. Start the event-routing query
        # -------------------------------------------------------------------

        routing_query = (
            events.writeStream
            .queryName("iot_event_quality_routing")
            .foreachBatch(route_batch)
            .option(
                "checkpointLocation",
                CHECKPOINT_DIR.as_posix(),
            )
            .trigger(
                processingTime=PROCESSING_INTERVAL,
            )
            .start()
        )

        # -------------------------------------------------------------------
        # 8. Start the sliding-window metrics query
        # -------------------------------------------------------------------

        metrics_query = (
            windowed_metrics.writeStream
            .queryName("iot_sensor_window_metrics")
            .foreachBatch(persist_window_metrics_batch)
            .outputMode(
                "append"
                if WINDOW_PERSISTENCE_MODE == "silo_summaries"
                else "update"
            )
            .option(
                "checkpointLocation",
                WINDOW_CHECKPOINT_DIR.as_posix(),
            )
            .trigger(
                processingTime=PROCESSING_INTERVAL,
            )
            .start()
        )

        logger.info(
            "Event-routing query started: %s",
            routing_query.id,
        )
        logger.info(
            "Window-metrics query started: %s",
            metrics_query.id,
        )
        logger.info("Press Ctrl+C to stop.")

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Streaming shutdown requested.")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
