
"""PostgreSQL persistence helpers for IoT streaming data."""

import logging
import os
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

logger = logging.getLogger(__name__)


def get_postgres_connection():
    """Create a PostgreSQL connection using environment variables."""
    load_dotenv(ENV_FILE)

    required_variables = (
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    )
    missing = [
        name for name in required_variables
        if not os.getenv(name)
    ]

    if missing:
        raise RuntimeError(
            f"Missing PostgreSQL environment variables: {missing}"
        )

    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
    )


def _as_dict(row: Any) -> dict:
    """Convert a Spark Row or mapping into a dictionary."""
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)


def persist_sensor_events_partition(
    partition_rows: Iterable[Any],
) -> None:
    """Persist accepted Kafka events without duplicating Kafka offsets."""
    rows = [_as_dict(row) for row in partition_rows]

    if not rows:
        return

    sql = """
    INSERT INTO sensor_events (
        topic,
        partition_id,
        kafka_offset,
        schema_version,
        sensor_id,
        location_id,
        event_timestamp,
        kafka_timestamp,
        latitude,
        longitude,
        pressure_pa,
        temperature_c,
        humidity_pct,
        pressure_status,
        temperature_status,
        humidity_status,
        record_status,
        processing_status,
        location_geom
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        CASE
        WHEN %s IS NOT NULL AND %s IS NOT NULL
        THEN ST_SetSRID(
            ST_MakePoint(%s, %s), 4326
        )
        ELSE NULL
        END
    )
    ON CONFLICT (topic, partition_id, kafka_offset)
    DO NOTHING
    """

    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                for row in rows:
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")

                    cursor.execute(
                        sql,
                        (
                            row.get("topic"),
                            row.get("partition"),
                            row.get("offset"),
                            row.get("schema_version"),
                            row.get("sensor_id"),
                            row.get("location_id"),
                            row.get("event_timestamp"),
                            row.get("kafka_timestamp"),
                            latitude,
                            longitude,
                            row.get("pressure_pa_candidate"),
                            row.get("temperature_c"),
                            row.get("humidity_pct"),
                            row.get("pressure_status"),
                            row.get("temperature_status"),
                            row.get("humidity_status"),
                            row.get("record_status"),
                            row.get("processing_status"),
                            latitude,
                            longitude,
                            longitude,
                            latitude,
                        ),
                    )
    finally:
        connection.close()

        logger.info("Processed %s event candidates.", len(rows))


def persist_window_metrics_partition(
    partition_rows: Iterable[Any],
) -> None:
    """Insert or update window metrics by topic and sensor window."""
    rows = [_as_dict(row) for row in partition_rows]

    if not rows:
        return

    sql = """
    INSERT INTO sensor_window_metrics (
        topic,
        sensor_id,
        location_id,
        window_start,
        window_end,
        latitude,
        longitude,
        measurement_count,
        avg_pressure_pa,
        min_pressure_pa,
        max_pressure_pa,
        avg_temperature_c,
        min_temperature_c,
        max_temperature_c,
        avg_humidity_pct,
        min_humidity_pct,
        max_humidity_pct,
        pressure_range_pa,
        temperature_range_c,
        humidity_range_pct,
        pressure_variation_anomaly,
        temperature_variation_anomaly,
        humidity_variation_anomaly,
        anomaly_status,
        location_geom
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        CASE
        WHEN %s IS NOT NULL AND %s IS NOT NULL
        THEN ST_SetSRID(
            ST_MakePoint(%s, %s), 4326
        )
        ELSE NULL
        END
    )
    ON CONFLICT (topic, sensor_id, window_start, window_end)
    DO UPDATE SET
    location_id = EXCLUDED.location_id,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    measurement_count = EXCLUDED.measurement_count,
    avg_pressure_pa = EXCLUDED.avg_pressure_pa,
    min_pressure_pa = EXCLUDED.min_pressure_pa,
    max_pressure_pa = EXCLUDED.max_pressure_pa,
    avg_temperature_c = EXCLUDED.avg_temperature_c,
    min_temperature_c = EXCLUDED.min_temperature_c,
    max_temperature_c = EXCLUDED.max_temperature_c,
    avg_humidity_pct = EXCLUDED.avg_humidity_pct,
    min_humidity_pct = EXCLUDED.min_humidity_pct,
    max_humidity_pct = EXCLUDED.max_humidity_pct,
    pressure_range_pa = EXCLUDED.pressure_range_pa,
    temperature_range_c = EXCLUDED.temperature_range_c,
    humidity_range_pct = EXCLUDED.humidity_range_pct,
    pressure_variation_anomaly =
    EXCLUDED.pressure_variation_anomaly,
    temperature_variation_anomaly =
    EXCLUDED.temperature_variation_anomaly,
    humidity_variation_anomaly =
    EXCLUDED.humidity_variation_anomaly,
    anomaly_status = EXCLUDED.anomaly_status,
    location_geom = EXCLUDED.location_geom
    """

    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                for row in rows:
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")

                    cursor.execute(
                        sql,
                        (
                            row.get("topic"),
                            row.get("sensor_id"),
                            row.get("location_id"),
                            row.get("window_start"),
                            row.get("window_end"),
                            latitude,
                            longitude,
                            row.get("measurement_count"),
                            row.get("avg_pressure_pa"),
                            row.get("min_pressure_pa"),
                            row.get("max_pressure_pa"),
                            row.get("avg_temperature_c"),
                            row.get("min_temperature_c"),
                            row.get("max_temperature_c"),
                            row.get("avg_humidity_pct"),
                            row.get("min_humidity_pct"),
                            row.get("max_humidity_pct"),
                            row.get("pressure_range_pa"),
                            row.get("temperature_range_c"),
                            row.get("humidity_range_pct"),
                            row.get("pressure_variation_anomaly"),
                            row.get("temperature_variation_anomaly"),
                            row.get("humidity_variation_anomaly"),
                            row.get("anomaly_status"),
                            latitude,
                            longitude,
                            longitude,
                            latitude,
                        ),
                    )
    finally:
        connection.close()

        logger.info("Persisted %s window metrics.", len(rows))
