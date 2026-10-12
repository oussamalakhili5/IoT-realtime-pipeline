
"""Test Spark read/write access to Silo through the S3A connector."""

import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env.docker")

ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

if not ACCESS_KEY or not SECRET_KEY:
    raise RuntimeError(
        "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set "
        "in the project-root .env.docker file."
    )

OUTPUT_PATH = (
    "s3a://iot-data-lake/processed/spark-connectivity-test/"
)


def main():
    """Write and read a small Parquet dataset in Silo."""
    spark = (
        SparkSession.builder
        .appName("IoT-Silo-S3-Connectivity-Test")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000",
        )
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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


    try:
        test_df = spark.sql("""
        SELECT
        'SILO_TEST_001' AS sensor_id,
        CAST(21.7 AS DOUBLE) AS temperature_c,
        CAST(45.2 AS DOUBLE) AS humidity_pct,
        current_timestamp() AS event_timestamp
        """)

        print("Writing test Parquet data to Silo...")
        test_df.write.mode("overwrite").parquet(OUTPUT_PATH)

        print("Reading test Parquet data back from Silo...")
        result_df = spark.read.parquet(OUTPUT_PATH)

        result_df.show(truncate=False)

        assert result_df.count() == 1, (
            "Expected exactly one test record."
        )

        print("SUCCESS: Spark can write and read Parquet in Silo.")
        print(f"Location: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
