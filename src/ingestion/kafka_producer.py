
"""Publish quality-assessed IoT events to Apache Kafka."""

import argparse
import json
import logging
import math
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from src.streaming.event_schema import build_event
from src.streaming.measurement_rules import assess_measurement


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2026-03_bme280"
    / "2026-03_bme280.csv"
)

DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "iot-sensor-events"
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_MAX_MESSAGES = 100

REPORTING_PERIOD_START = pd.Timestamp("2026-03-01")
REPORTING_PERIOD_END = pd.Timestamp("2026-04-01")

REQUIRED_COLUMNS = {
    "sensor_id",
    "timestamp",
    "pressure",
    "temperature",
    "humidity",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class OutsideReportingPeriodError(ValueError):
    """Raised when a record falls outside the reporting period."""


def clean_text(value):
    """Return cleaned text or None for missing values."""

    if pd.isna(value):
        return None

    text = str(value).strip().strip("'\"").strip()

    return text or None


def to_optional_float(value):
    """Convert a value to a finite float or None."""

    text = clean_text(value)

    if text is None:
        return None

    try:
        number = float(text)
    except (TypeError, ValueError, OverflowError):
        return None

    return number if math.isfinite(number) else None


def build_event_from_source_row(row):
    """Convert one raw CSV record into a canonical IoT event."""

    sensor_id = clean_text(row.get("sensor_id"))
    timestamp = clean_text(row.get("timestamp"))

    if sensor_id is None:
        raise ValueError("Missing sensor_id")

    if timestamp is None:
        raise ValueError("Missing timestamp")

    parsed_timestamp = pd.to_datetime(
        timestamp,
        format="ISO8601",
        errors="coerce",
    )

    if pd.isna(parsed_timestamp):
        raise ValueError("Invalid timestamp")

    if not (
        REPORTING_PERIOD_START
        <= parsed_timestamp
        < REPORTING_PERIOD_END
    ):
        raise OutsideReportingPeriodError(
            f"Timestamp outside March 2026: {timestamp}"
        )

    pressure_raw = clean_text(row.get("pressure"))
    temperature = to_optional_float(row.get("temperature"))
    humidity = to_optional_float(row.get("humidity"))

    quality = assess_measurement(
        pressure=pressure_raw,
        temperature=temperature,
        humidity=humidity,
    )

    event_input = {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "pressure_raw": pressure_raw,
        "pressure_numeric": to_optional_float(pressure_raw),
        **quality,
    }

    return build_event(event_input)


def parse_arguments():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Publish IoT sensor events to Apache Kafka."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help="Path to the raw sensor CSV file.",
    )

    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Kafka bootstrap server address.",
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka destination topic.",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of CSV rows read per batch.",
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=DEFAULT_MAX_MESSAGES,
        help="Maximum number of events to publish.",
    )

    return parser.parse_args()


def main():
    """Read source records and publish canonical events."""

    args = parse_arguments()

    if not args.input.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {args.input}"
        )

    if args.chunk_size < 1 or args.max_messages < 1:
        raise ValueError(
            "chunk-size and max-messages must both be positive."
        )

    header = pd.read_csv(
        args.input,
        sep=";",
        nrows=0,
        encoding="utf-8",
    )

    column_mapping = {
        str(column).strip(): column
        for column in header.columns
    }

    missing_columns = REQUIRED_COLUMNS - set(column_mapping)

    if missing_columns:
        raise ValueError(
            f"Missing source columns: {sorted(missing_columns)}. "
            f"Available columns: {list(column_mapping)}"
        )

    selected_columns = [
        column_mapping[name]
        for name in (
            "sensor_id",
            "timestamp",
            "pressure",
            "temperature",
            "humidity",
        )
    ]

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        client_id="iot-realtime-pipeline-producer",
        acks="all",
        retries=5,
        linger_ms=20,
        compression_type="gzip",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda event: json.dumps(
            event,
            allow_nan=False,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    rows_inspected = 0
    events_published = 0
    rows_skipped = 0
    rows_outside_period = 0
    stop_requested = False

    try:
        chunks = pd.read_csv(
            args.input,
            sep=";",
            usecols=selected_columns,
            dtype="string",
            chunksize=args.chunk_size,
            encoding="utf-8",
        )

        for chunk_number, chunk in enumerate(chunks, start=1):
            chunk.columns = [
                str(column).strip()
                for column in chunk.columns
            ]

            pending_futures = []

            for values in chunk.itertuples(
                index=False,
                name=None,
            ):
                if events_published >= args.max_messages:
                    stop_requested = True
                    break

                rows_inspected += 1
                row = dict(zip(chunk.columns, values))

                try:
                    event = build_event_from_source_row(row)

                except OutsideReportingPeriodError as exc:
                    rows_outside_period += 1
                    rows_skipped += 1

                    if rows_outside_period <= 5:
                        logger.warning(
                            "Record outside reporting period: %s",
                            exc,
                        )

                    continue

                except (TypeError, ValueError) as exc:
                    rows_skipped += 1

                    if rows_skipped <= 5:
                        logger.warning(
                            "Skipping source row %d: %s",
                            rows_inspected,
                            exc,
                        )

                    continue

                future = producer.send(
                    args.topic,
                    key=event["sensor_id"],
                    value=event,
                )

                pending_futures.append(future)
                events_published += 1

            producer.flush()

            for future in pending_futures:
                future.get(timeout=30)

            logger.info(
                "Batch %d completed | inspected=%d | published=%d "
                "| skipped=%d | outside_period=%d",
                chunk_number,
                rows_inspected,
                events_published,
                rows_skipped,
                rows_outside_period,
            )

            if stop_requested:
                break

    finally:
        producer.close()

    logger.info("Producer execution completed.")
    logger.info("Rows inspected: %d", rows_inspected)
    logger.info("Events published: %d", events_published)
    logger.info("Rows skipped: %d", rows_skipped)
    logger.info(
        "Rows outside March 2026: %d",
        rows_outside_period,
    )
    logger.info("Topic: %s", args.topic)


if __name__ == "__main__":
    main()
