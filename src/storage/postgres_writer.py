
"""PostgreSQL persistence helpers for IoT streaming data."""

import hashlib
import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

logger = logging.getLogger(__name__)


def _normalize_location_component(value):
    """Return a stable JSON-compatible representation of a location value."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        value = float(value)

        if math.isnan(value):
            return {"special_float": "nan"}

        if math.isinf(value):
            return {
                "special_float": "positive_infinity"
                if value > 0
                else "negative_infinity"
            }

        # Normalize negative zero so equivalent coordinates share a key.
        if value == 0:
            return 0.0

        return value

    return str(value)


def _location_key(row):
    """Build a deterministic key from the current Spark location grouping."""
    record = _as_dict(row)

    location_id = record.get("location_id")
    if location_id is not None:
        location_id = str(location_id)

    canonical_location = [
        location_id,
        _normalize_location_component(record.get("latitude")),
        _normalize_location_component(record.get("longitude")),
    ]

    payload = json.dumps(
        canonical_location,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    return "loc1:" + hashlib.sha256(payload).hexdigest()


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


def _as_utc_timestamp(value: Any) -> Any:
    """Make Spark's naive UTC timestamps explicit for timestamptz columns."""
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


_CLAIM_BATCH_SQL = """
INSERT INTO iot_stream_batch_progress (
    topic, run_id, sink_name, last_batch_id
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (topic, run_id, sink_name)
DO UPDATE SET
    last_batch_id = EXCLUDED.last_batch_id,
    updated_at = CURRENT_TIMESTAMP
WHERE iot_stream_batch_progress.last_batch_id
    < EXCLUDED.last_batch_id
RETURNING last_batch_id
"""


def _claim_stream_batch(cursor, topic, run_id, sink_name, batch_id):
    """Claim a monotonically increasing Spark batch inside its transaction."""
    cursor.execute(
        _CLAIM_BATCH_SQL,
        (topic, run_id, sink_name, batch_id),
    )
    return cursor.fetchone() is not None


def _metric_values(row: dict) -> tuple:
    """Return the shared metric fields used by compact event summaries."""
    return (
        row.get("accepted_event_count", 0),
        row.get("pressure_observation_count", 0),
        row.get("pressure_sum"),
        row.get("pressure_min"),
        row.get("pressure_max"),
        row.get("temperature_observation_count", 0),
        row.get("temperature_sum"),
        row.get("temperature_min"),
        row.get("temperature_max"),
        row.get("humidity_observation_count", 0),
        row.get("humidity_sum"),
        row.get("humidity_min"),
        row.get("humidity_max"),
    )


_UPSERT_HOURLY_SUMMARY_SQL = """
INSERT INTO iot_global_hourly_summaries (
    topic, run_id, hour_start, hour_end,
    accepted_event_count,
    pressure_observation_count, pressure_sum, pressure_min, pressure_max,
    temperature_observation_count, temperature_sum,
    temperature_min, temperature_max,
    humidity_observation_count, humidity_sum, humidity_min, humidity_max
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (topic, run_id, hour_start)
DO UPDATE SET
    hour_end = EXCLUDED.hour_end,
    accepted_event_count =
        iot_global_hourly_summaries.accepted_event_count
        + EXCLUDED.accepted_event_count,
    pressure_observation_count =
        iot_global_hourly_summaries.pressure_observation_count
        + EXCLUDED.pressure_observation_count,
    pressure_sum = COALESCE(iot_global_hourly_summaries.pressure_sum, 0)
        + COALESCE(EXCLUDED.pressure_sum, 0),
    pressure_min = CASE
        WHEN iot_global_hourly_summaries.pressure_min IS NULL
            THEN EXCLUDED.pressure_min
        WHEN EXCLUDED.pressure_min IS NULL
            THEN iot_global_hourly_summaries.pressure_min
        ELSE LEAST(iot_global_hourly_summaries.pressure_min,
                   EXCLUDED.pressure_min)
    END,
    pressure_max = CASE
        WHEN iot_global_hourly_summaries.pressure_max IS NULL
            THEN EXCLUDED.pressure_max
        WHEN EXCLUDED.pressure_max IS NULL
            THEN iot_global_hourly_summaries.pressure_max
        ELSE GREATEST(iot_global_hourly_summaries.pressure_max,
                      EXCLUDED.pressure_max)
    END,
    temperature_observation_count =
        iot_global_hourly_summaries.temperature_observation_count
        + EXCLUDED.temperature_observation_count,
    temperature_sum =
        COALESCE(iot_global_hourly_summaries.temperature_sum, 0)
        + COALESCE(EXCLUDED.temperature_sum, 0),
    temperature_min = CASE
        WHEN iot_global_hourly_summaries.temperature_min IS NULL
            THEN EXCLUDED.temperature_min
        WHEN EXCLUDED.temperature_min IS NULL
            THEN iot_global_hourly_summaries.temperature_min
        ELSE LEAST(iot_global_hourly_summaries.temperature_min,
                   EXCLUDED.temperature_min)
    END,
    temperature_max = CASE
        WHEN iot_global_hourly_summaries.temperature_max IS NULL
            THEN EXCLUDED.temperature_max
        WHEN EXCLUDED.temperature_max IS NULL
            THEN iot_global_hourly_summaries.temperature_max
        ELSE GREATEST(iot_global_hourly_summaries.temperature_max,
                      EXCLUDED.temperature_max)
    END,
    humidity_observation_count =
        iot_global_hourly_summaries.humidity_observation_count
        + EXCLUDED.humidity_observation_count,
    humidity_sum = COALESCE(iot_global_hourly_summaries.humidity_sum, 0)
        + COALESCE(EXCLUDED.humidity_sum, 0),
    humidity_min = CASE
        WHEN iot_global_hourly_summaries.humidity_min IS NULL
            THEN EXCLUDED.humidity_min
        WHEN EXCLUDED.humidity_min IS NULL
            THEN iot_global_hourly_summaries.humidity_min
        ELSE LEAST(iot_global_hourly_summaries.humidity_min,
                   EXCLUDED.humidity_min)
    END,
    humidity_max = CASE
        WHEN iot_global_hourly_summaries.humidity_max IS NULL
            THEN EXCLUDED.humidity_max
        WHEN EXCLUDED.humidity_max IS NULL
            THEN iot_global_hourly_summaries.humidity_max
        ELSE GREATEST(iot_global_hourly_summaries.humidity_max,
                      EXCLUDED.humidity_max)
    END
"""


_UPSERT_DAILY_SENSOR_SUMMARY_SQL = """
INSERT INTO iot_daily_sensor_summaries (
    topic, run_id, summary_date, sensor_id, location_key,
    location_id, latitude, longitude,
    accepted_event_count,
    pressure_observation_count, pressure_sum, pressure_min, pressure_max,
    temperature_observation_count, temperature_sum,
    temperature_min, temperature_max,
    humidity_observation_count, humidity_sum, humidity_min, humidity_max
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON CONFLICT (topic, run_id, summary_date, sensor_id, location_key)
DO UPDATE SET
    location_id = EXCLUDED.location_id,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    accepted_event_count =
        iot_daily_sensor_summaries.accepted_event_count
        + EXCLUDED.accepted_event_count,
    pressure_observation_count =
        iot_daily_sensor_summaries.pressure_observation_count
        + EXCLUDED.pressure_observation_count,
    pressure_sum = COALESCE(iot_daily_sensor_summaries.pressure_sum, 0)
        + COALESCE(EXCLUDED.pressure_sum, 0),
    pressure_min = CASE
        WHEN iot_daily_sensor_summaries.pressure_min IS NULL
            THEN EXCLUDED.pressure_min
        WHEN EXCLUDED.pressure_min IS NULL
            THEN iot_daily_sensor_summaries.pressure_min
        ELSE LEAST(iot_daily_sensor_summaries.pressure_min,
                   EXCLUDED.pressure_min)
    END,
    pressure_max = CASE
        WHEN iot_daily_sensor_summaries.pressure_max IS NULL
            THEN EXCLUDED.pressure_max
        WHEN EXCLUDED.pressure_max IS NULL
            THEN iot_daily_sensor_summaries.pressure_max
        ELSE GREATEST(iot_daily_sensor_summaries.pressure_max,
                      EXCLUDED.pressure_max)
    END,
    temperature_observation_count =
        iot_daily_sensor_summaries.temperature_observation_count
        + EXCLUDED.temperature_observation_count,
    temperature_sum =
        COALESCE(iot_daily_sensor_summaries.temperature_sum, 0)
        + COALESCE(EXCLUDED.temperature_sum, 0),
    temperature_min = CASE
        WHEN iot_daily_sensor_summaries.temperature_min IS NULL
            THEN EXCLUDED.temperature_min
        WHEN EXCLUDED.temperature_min IS NULL
            THEN iot_daily_sensor_summaries.temperature_min
        ELSE LEAST(iot_daily_sensor_summaries.temperature_min,
                   EXCLUDED.temperature_min)
    END,
    temperature_max = CASE
        WHEN iot_daily_sensor_summaries.temperature_max IS NULL
            THEN EXCLUDED.temperature_max
        WHEN EXCLUDED.temperature_max IS NULL
            THEN iot_daily_sensor_summaries.temperature_max
        ELSE GREATEST(iot_daily_sensor_summaries.temperature_max,
                      EXCLUDED.temperature_max)
    END,
    humidity_observation_count =
        iot_daily_sensor_summaries.humidity_observation_count
        + EXCLUDED.humidity_observation_count,
    humidity_sum = COALESCE(iot_daily_sensor_summaries.humidity_sum, 0)
        + COALESCE(EXCLUDED.humidity_sum, 0),
    humidity_min = CASE
        WHEN iot_daily_sensor_summaries.humidity_min IS NULL
            THEN EXCLUDED.humidity_min
        WHEN EXCLUDED.humidity_min IS NULL
            THEN iot_daily_sensor_summaries.humidity_min
        ELSE LEAST(iot_daily_sensor_summaries.humidity_min,
                   EXCLUDED.humidity_min)
    END,
    humidity_max = CASE
        WHEN iot_daily_sensor_summaries.humidity_max IS NULL
            THEN EXCLUDED.humidity_max
        WHEN EXCLUDED.humidity_max IS NULL
            THEN iot_daily_sensor_summaries.humidity_max
        ELSE GREATEST(iot_daily_sensor_summaries.humidity_max,
                      EXCLUDED.humidity_max)
    END
"""


def persist_event_summary_batch(
    topic: str,
    run_id: str,
    batch_id: int,
    hourly_rows: Iterable[Any],
    daily_sensor_rows: Iterable[Any],
) -> bool:
    """Add accepted-event summaries once per routing micro-batch.

    The batch claim and summary upserts share one PostgreSQL transaction. A
    replayed Spark batch therefore either repeats all writes after rollback or
    skips all writes after a successful commit.
    """
    hourly = [_as_dict(row) for row in hourly_rows]
    daily = [_as_dict(row) for row in daily_sensor_rows]
    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                if not _claim_stream_batch(
                    cursor, topic, run_id, "event_summaries", batch_id
                ):
                    logger.info(
                        "Skipped already committed event summary batch %s",
                        batch_id,
                    )
                    return False

                for row in hourly:
                    cursor.execute(
                        _UPSERT_HOURLY_SUMMARY_SQL,
                        (
                            topic,
                            run_id,
                            _as_utc_timestamp(row.get("hour_start")),
                            _as_utc_timestamp(row.get("hour_end")),
                            *_metric_values(row),
                        ),
                    )

                for row in daily:
                    cursor.execute(
                        _UPSERT_DAILY_SENSOR_SUMMARY_SQL,
                        (
                            topic,
                            run_id,
                            row.get("summary_date"),
                            row.get("sensor_id"),
                            _location_key(row),
                            row.get("location_id"),
                            row.get("latitude"),
                            row.get("longitude"),
                            *_metric_values(row),
                        ),
                    )
        logger.info(
            "Persisted event summaries for batch %s: hourly=%d daily_sensor=%d",
            batch_id,
            len(hourly),
            len(daily),
        )
        return True
    finally:
        connection.close()


_UPSERT_WINDOW_STATUS_HOURLY_SQL = """
INSERT INTO iot_window_status_hourly (
    topic, run_id, hour_start,
    normal_count, anomaly_suspected_count, insufficient_data_count,
    other_count
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (topic, run_id, hour_start)
DO UPDATE SET
    normal_count = iot_window_status_hourly.normal_count
        + EXCLUDED.normal_count,
    anomaly_suspected_count =
        iot_window_status_hourly.anomaly_suspected_count
        + EXCLUDED.anomaly_suspected_count,
    insufficient_data_count =
        iot_window_status_hourly.insufficient_data_count
        + EXCLUDED.insufficient_data_count,
    other_count = iot_window_status_hourly.other_count
        + EXCLUDED.other_count
"""


_INSERT_WINDOW_ANOMALY_SQL = """
INSERT INTO iot_sensor_window_anomalies (
    topic, run_id, sensor_id, location_key, location_id,
    window_start, window_end,
    latitude, longitude, measurement_count,
    pressure_observation_count, avg_pressure_pa, min_pressure_pa,
    max_pressure_pa, pressure_range_pa, pressure_variation_anomaly,
    temperature_observation_count, avg_temperature_c, min_temperature_c,
    max_temperature_c, temperature_range_c, temperature_variation_anomaly,
    humidity_observation_count, avg_humidity_pct, min_humidity_pct,
    max_humidity_pct, humidity_range_pct, humidity_variation_anomaly,
    anomaly_status
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (
    topic, run_id, sensor_id, location_key, window_start, window_end
)
DO UPDATE SET
    location_id = EXCLUDED.location_id,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    measurement_count = EXCLUDED.measurement_count,
    pressure_observation_count = EXCLUDED.pressure_observation_count,
    avg_pressure_pa = EXCLUDED.avg_pressure_pa,
    min_pressure_pa = EXCLUDED.min_pressure_pa,
    max_pressure_pa = EXCLUDED.max_pressure_pa,
    pressure_range_pa = EXCLUDED.pressure_range_pa,
    pressure_variation_anomaly = EXCLUDED.pressure_variation_anomaly,
    temperature_observation_count = EXCLUDED.temperature_observation_count,
    avg_temperature_c = EXCLUDED.avg_temperature_c,
    min_temperature_c = EXCLUDED.min_temperature_c,
    max_temperature_c = EXCLUDED.max_temperature_c,
    temperature_range_c = EXCLUDED.temperature_range_c,
    temperature_variation_anomaly = EXCLUDED.temperature_variation_anomaly,
    humidity_observation_count = EXCLUDED.humidity_observation_count,
    avg_humidity_pct = EXCLUDED.avg_humidity_pct,
    min_humidity_pct = EXCLUDED.min_humidity_pct,
    max_humidity_pct = EXCLUDED.max_humidity_pct,
    humidity_range_pct = EXCLUDED.humidity_range_pct,
    humidity_variation_anomaly = EXCLUDED.humidity_variation_anomaly,
    anomaly_status = EXCLUDED.anomaly_status
"""


def persist_window_summary_batch(
    topic: str,
    run_id: str,
    batch_id: int,
    hourly_status_rows: Iterable[Any],
    anomaly_rows: Iterable[Any],
) -> bool:
    """Persist window status counts and anomaly details once per batch."""
    hourly_status = [_as_dict(row) for row in hourly_status_rows]
    anomalies = [_as_dict(row) for row in anomaly_rows]
    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                if not _claim_stream_batch(
                    cursor, topic, run_id, "window_summaries", batch_id
                ):
                    logger.info(
                        "Skipped already committed window summary batch %s",
                        batch_id,
                    )
                    return False

                for row in hourly_status:
                    cursor.execute(
                        _UPSERT_WINDOW_STATUS_HOURLY_SQL,
                        (
                            topic,
                            run_id,
                            _as_utc_timestamp(row.get("hour_start")),
                            row.get("normal_count", 0),
                            row.get("anomaly_suspected_count", 0),
                            row.get("insufficient_data_count", 0),
                            row.get("other_count", 0),
                        ),
                    )

                for row in anomalies:
                    cursor.execute(
                        _INSERT_WINDOW_ANOMALY_SQL,
                        (
                            topic,
                            run_id,
                            row.get("sensor_id"),
                            _location_key(row),
                            row.get("location_id"),
                            _as_utc_timestamp(row.get("window_start")),
                            _as_utc_timestamp(row.get("window_end")),
                            row.get("latitude"),
                            row.get("longitude"),
                            row.get("measurement_count"),
                            row.get("pressure_observation_count"),
                            row.get("avg_pressure_pa"),
                            row.get("min_pressure_pa"),
                            row.get("max_pressure_pa"),
                            row.get("pressure_range_pa"),
                            row.get("pressure_variation_anomaly"),
                            row.get("temperature_observation_count"),
                            row.get("avg_temperature_c"),
                            row.get("min_temperature_c"),
                            row.get("max_temperature_c"),
                            row.get("temperature_range_c"),
                            row.get("temperature_variation_anomaly"),
                            row.get("humidity_observation_count"),
                            row.get("avg_humidity_pct"),
                            row.get("min_humidity_pct"),
                            row.get("max_humidity_pct"),
                            row.get("humidity_range_pct"),
                            row.get("humidity_variation_anomaly"),
                            row.get("anomaly_status"),
                        ),
                    )
        logger.info(
            "Persisted window summaries for batch %s: status_hours=%d anomalies=%d",
            batch_id,
            len(hourly_status),
            len(anomalies),
        )
        return True
    finally:
        connection.close()
