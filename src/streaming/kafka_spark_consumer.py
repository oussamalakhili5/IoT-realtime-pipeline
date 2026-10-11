"""Consume, validate, and analyze IoT events with Spark Structured Streaming."""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    length,
    lit,
    max as spark_max,
    min as spark_min,
    stddev_pop,
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
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-events-march-geospatial-test"

WINDOW_DURATION = "10 minutes"
WINDOW_SLIDE = "5 minutes"
WATERMARK_DELAY = "10 minutes"

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "checkpoints"
    / "spark_iot_kafka_march_anomaly_routing_v1"
)

WINDOW_CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "checkpoints"
    / "spark_iot_kafka_march_anomaly_windows_v1"
)

MAX_OFFSETS_PER_TRIGGER = 100
PROCESSING_INTERVAL = "5 seconds"


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


# ---------------------------------------------------------------------------
# Event routing
# ---------------------------------------------------------------------------

def route_batch(batch_df, batch_id):
    """Route each micro-batch into accepted and review-required events."""

    accepted_events = batch_df.filter(
        col("processing_status") == "accepted"
    )

    review_events = batch_df.filter(
        col("processing_status") != "accepted"
    )

    logger.info("Routing micro-batch %s", batch_id)

    print(f"\n=== BATCH {batch_id}: ACCEPTED EVENTS ===")
    accepted_events.show(n=10, truncate=False)

    print(f"\n=== BATCH {batch_id}: EVENTS REQUIRING REVIEW ===")
    review_events.show(n=10, truncate=False)


    # ---------------------------------------------------------------------------
    # Main streaming application
    # ---------------------------------------------------------------------------

def main():
    """Start Kafka ingestion, event routing, and windowed aggregations."""

    spark = (
        SparkSession.builder
        .appName("IoT-Real-Time-Kafka-Consumer")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

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
                "event",
                from_json(col("json_payload"), EVENT_SCHEMA),
            )
        )

        # -------------------------------------------------------------------
        # 3. Flatten event fields and assess processing status
        # -------------------------------------------------------------------

        events = (
            parsed_stream
            .select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("event").isNotNull().alias("json_parsed"),
                col("event.schema_version").alias("schema_version"),
                col("event.sensor_id").alias("sensor_id"),
                col("event.timestamp").alias("event_timestamp_raw"),
                col(
                    "event.location.location_id"
                ).alias("location_id"),
                col(
                    "event.location.latitude"
                ).alias("latitude"),
                col(
                    "event.location.longitude"
                ).alias("longitude"),
                col(
                    "event.measurements.pressure_pa_candidate"
                ).alias("pressure_pa_candidate"),
                col(
                    "event.measurements.temperature_c"
                ).alias("temperature_c"),
                col(
                    "event.measurements.humidity_pct"
                ).alias("humidity_pct"),
                col(
                    "event.quality.pressure_status"
                ).alias("pressure_status"),
                col(
                    "event.quality.temperature_status"
                ).alias("temperature_status"),
                col(
                    "event.quality.humidity_status"
                ).alias("humidity_status"),
                col(
                    "event.quality.humidity_at_boundary"
                ).alias("humidity_at_boundary"),
                col(
                    "event.quality.record_status"
                ).alias("record_status"),
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
                    | (
                        ~col("schema_version").isin("1.0", "1.1")
                    ),
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
                .otherwise(
                    lit("unknown_quality_status")
                ),
            )
        )

        # -------------------------------------------------------------------
        # 4. Route accepted events to the analytical stream
        # -------------------------------------------------------------------

        accepted_events = events.filter(
            (col("processing_status") == "accepted")
            & col("event_timestamp").isNotNull()
        )

        # -------------------------------------------------------------------
        # 5. Calculate sliding-window statistics per sensor
        # -------------------------------------------------------------------

        windowed_metrics = (
            accepted_events
            .withWatermark(
                "event_timestamp",
                WATERMARK_DELAY,
            )
            .groupBy(
                window(
                    col("event_timestamp"),
                    WINDOW_DURATION,
                    WINDOW_SLIDE,
                ),
                col("sensor_id"),
                col("location_id"),
                col("latitude"),
                col("longitude"),
            )
            .agg(
                count("*").alias("measurement_count"),

                avg("pressure_pa_candidate").alias(
                    "avg_pressure_pa"
                ),
                spark_min("pressure_pa_candidate").alias(
                    "min_pressure_pa"
                ),
                spark_max("pressure_pa_candidate").alias(
                    "max_pressure_pa"
                ),
                stddev_pop("pressure_pa_candidate").alias(
                    "stddev_pressure_pa"
                ),

                avg("temperature_c").alias(
                    "avg_temperature_c"
                ),
                spark_min("temperature_c").alias(
                    "min_temperature_c"
                ),
                spark_max("temperature_c").alias(
                    "max_temperature_c"
                ),
                stddev_pop("temperature_c").alias(
                    "stddev_temperature_c"
                ),

                avg("humidity_pct").alias(
                    "avg_humidity_pct"
                ),
                spark_min("humidity_pct").alias(
                    "min_humidity_pct"
                ),
                spark_max("humidity_pct").alias(
                    "max_humidity_pct"
                ),
                stddev_pop("humidity_pct").alias(
                    "stddev_humidity_pct"
                ),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
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
            )
        )

        # -------------------------------------------------------------------
        # 6. Prepare checkpoint parent directories
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
        # 8. Start the sliding-window aggregation query
        # -------------------------------------------------------------------

        metrics_query = (
            windowed_metrics.writeStream
            .queryName("iot_sensor_window_metrics")
            .format("console")
            .outputMode("update")
            .option("truncate", "false")
            .option("numRows", "50")
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