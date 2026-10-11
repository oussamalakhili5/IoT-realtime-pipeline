
"""Consume and parse IoT events from Kafka using Spark Structured Streaming."""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    length,
    lit,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-events-march-validated"

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "checkpoints"
    / "spark_iot_kafka_march_transformations_v1"
)


EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
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


def main():
    """Start the Kafka source and process events with Spark."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    spark = (
        SparkSession.builder
        .appName("IoT-Real-Time-Kafka-Consumer")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        logging.info("Starting Spark Structured Streaming.")
        logging.info("Kafka server: %s", KAFKA_BOOTSTRAP_SERVERS)
        logging.info("Kafka topic: %s", KAFKA_TOPIC)

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "100")
            .load()
        )

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

        events = (
    parsed_stream.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("event").isNotNull().alias("json_parsed"),
        col("event.schema_version").alias("schema_version"),
        col("event.sensor_id").alias("sensor_id"),
        col("event.timestamp").alias("event_timestamp_raw"),
        col(
            "event.measurements.pressure_pa_candidate"
        ).alias("pressure_pa_candidate"),
        col("event.measurements.temperature_c").alias(
            "temperature_c"
        ),
        col("event.measurements.humidity_pct").alias(
            "humidity_pct"
        ),
        col("event.quality.pressure_status").alias(
            "pressure_status"
        ),
        col("event.quality.temperature_status").alias(
            "temperature_status"
        ),
        col("event.quality.humidity_status").alias(
            "humidity_status"
        ),
        col("event.quality.humidity_at_boundary").alias(
            "humidity_at_boundary"
        ),
        col("event.quality.record_status").alias(
            "record_status"
        ),
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
            | (col("schema_version") != lit("1.0")),
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
            col("record_status") == lit("review_required"),
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

        CHECKPOINT_DIR.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        query = (
            events.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", "10")
            .option("checkpointLocation", CHECKPOINT_DIR.as_posix())
            .trigger(processingTime="5 seconds")
            .start()
        )

        logging.info("Streaming query started. Press Ctrl+C to stop.")

        query.awaitTermination()

    except KeyboardInterrupt:
        logging.info("Streaming shutdown requested.")

    finally:
        spark.stop()
        logging.info("Spark session stopped.")


if __name__ == "__main__":
    main()
