"""Publish quality-assessed IoT events to Apache Kafka."""

import argparse
import csv
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

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

DEFAULT_PROGRESS_FILE = (
    PROJECT_ROOT
    / "data"
    / "interim"
    / "checkpoints"
    / "kafka_producer_march_progress.json"
)

REPORTING_PERIOD_START = datetime(2026, 3, 1)
REPORTING_PERIOD_END = datetime(2026, 4, 1)

SOURCE_COLUMN_ORDER = (
    "sensor_id",
    "location",
    "lat",
    "lon",
    "timestamp",
    "pressure",
    "temperature",
    "humidity",
)

REQUIRED_COLUMNS = set(SOURCE_COLUMN_ORDER)

CSV_MISSING_TEXT_VALUES = frozenset({
    "",
    "#n/a",
    "#n/a n/a",
    "#na",
    "-1.#ind",
    "-1.#qnan",
    "-nan",
    "1.#ind",
    "1.#qnan",
    "<na>",
    "n/a",
    "na",
    "null",
    "nan",
    "none",
})


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class OutsideReportingPeriodError(ValueError):
    """Raised when a record falls outside the reporting period."""


def select_source_columns(fieldnames):
    """Validate and map normalized source names to CSV header names."""

    column_mapping = {
        str(column).strip(): column
        for column in (fieldnames or [])
    }
    missing_columns = REQUIRED_COLUMNS - set(column_mapping)

    if missing_columns:
        raise ValueError(
            f"Missing source columns: {sorted(missing_columns)}. "
            f"Available columns: {list(column_mapping)}"
        )

    return {
        name: column_mapping[name]
        for name in SOURCE_COLUMN_ORDER
    }


def iter_csv_batches(reader, selected_columns, chunk_size):
    """Yield normalized source-row dictionaries in bounded batches."""

    batch = []

    for source_row in reader:
        batch.append({
            name: source_row.get(source_name)
            for name, source_name in selected_columns.items()
        })

        if len(batch) == chunk_size:
            yield batch
            batch = []

    if batch:
        yield batch


def clean_text(value):
    """Return cleaned text or None for missing values."""

    if value is None:
        return None

    try:
        if math.isnan(value):
            return None
    except (TypeError, ValueError, OverflowError):
        pass

    text = str(value).strip().strip("'\"").strip()

    if text.casefold() in CSV_MISSING_TEXT_VALUES:
        return None

    return text


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

    iso_timestamp = (
        timestamp[:-1] + "+00:00"
        if timestamp.endswith(("Z", "z"))
        else timestamp
    )

    try:
        parsed_timestamp = datetime.fromisoformat(iso_timestamp)
    except ValueError as exc:
        raise ValueError("Invalid timestamp") from exc

    # Interpret explicit offsets consistently with Spark's UTC session.
    if parsed_timestamp.tzinfo is not None:
        parsed_timestamp = parsed_timestamp.astimezone(timezone.utc)
        parsed_timestamp = parsed_timestamp.replace(tzinfo=None)

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
        "timestamp": parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "location": clean_text(row.get("location")),
        "lat": to_optional_float(row.get("lat")),
        "lon": to_optional_float(row.get("lon")),
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
        help="Maximum number of events to publish in this run.",
    )

    parser.add_argument(
        "--skip-rows",
        type=int,
        default=0,
        help="Number of source data rows to skip before processing.",
    )

    parser.add_argument(
        "--progress-file",
        type=Path,
        default=DEFAULT_PROGRESS_FILE,
        help="Path to the producer progress checkpoint file.",
    )

    return parser.parse_args()


def load_start_row(
    progress_path,
    input_path,
    topic,
    requested_skip_rows,
):
    """Validate saved progress and determine the source row to resume from."""

    start_row = requested_skip_rows

    if not progress_path.exists():
        return start_row

    try:
        progress = json.loads(
            progress_path.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot read producer progress checkpoint: {progress_path}"
        ) from exc

    if not isinstance(progress, dict):
        raise ValueError(
            "The progress checkpoint must contain a JSON object."
        )

    if progress.get("topic") != topic:
        raise ValueError(
            "The progress checkpoint belongs to a different Kafka topic."
        )

    if progress.get("input_file") != str(input_path):
        raise ValueError(
            "The progress checkpoint belongs to a different input file."
        )

    saved_rows = progress.get("rows_processed")

    if (
        not isinstance(saved_rows, int)
        or isinstance(saved_rows, bool)
        or saved_rows < 0
    ):
        raise ValueError(
            "The progress checkpoint contains an invalid row position."
        )

    if requested_skip_rows not in (0, saved_rows):
        raise ValueError(
            "The supplied --skip-rows value conflicts with the saved "
            "progress checkpoint. Omit --skip-rows to resume automatically."
        )

    return saved_rows


def save_progress(
    progress_path,
    input_path,
    topic,
    rows_processed,
):
    """Atomically save the last fully confirmed source row position."""

    progress_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    progress_data = {
        "input_file": str(input_path),
        "topic": topic,
        "rows_processed": rows_processed,
    }

    temporary_path = progress_path.with_suffix(
        progress_path.suffix + ".tmp"
    )

    try:
        temporary_path.write_text(
            json.dumps(progress_data, indent=2),
            encoding="utf-8",
        )
        temporary_path.replace(progress_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def confirm_pending_sends(
    producer,
    pending_futures,
    progress_path,
    input_path,
    topic,
    last_checkpoint_position,
):
    """Wait for sends and checkpoint the confirmed source-row prefix.

    Each future is paired with its 1-based source data-row position. If Kafka
    fails to confirm one, rows before it in this batch are either invalid and
    intentionally skipped or have already been confirmed. The failed row is
    left for the next run.
    """

    if not pending_futures:
        return 0

    producer.flush()
    confirmed = 0

    for source_position, future in pending_futures:
        try:
            future.get(timeout=30)
        except Exception:
            confirmed_prefix = source_position - 1
            if confirmed_prefix > last_checkpoint_position:
                save_progress(
                    progress_path=progress_path,
                    input_path=input_path,
                    topic=topic,
                    rows_processed=confirmed_prefix,
                )
            raise

        confirmed += 1

    return confirmed


def confirm_and_checkpoint_prefix(
    producer,
    pending_futures,
    progress_path,
    input_path,
    topic,
    source_position,
    last_checkpoint_position,
):
    """Confirm earlier sends before checkpointing the row before a failure."""

    confirmed = confirm_pending_sends(
        producer=producer,
        pending_futures=pending_futures,
        progress_path=progress_path,
        input_path=input_path,
        topic=topic,
        last_checkpoint_position=last_checkpoint_position,
    )
    confirmed_prefix = source_position - 1

    if confirmed_prefix > last_checkpoint_position:
        save_progress(
            progress_path=progress_path,
            input_path=input_path,
            topic=topic,
            rows_processed=confirmed_prefix,
        )

    return confirmed


def main():
    """Read source records and publish canonical events."""

    args = parse_arguments()

    input_path = args.input.resolve()
    progress_path = args.progress_file.resolve()

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {input_path}"
        )

    if args.chunk_size < 1 or args.max_messages < 1:
        raise ValueError(
            "chunk-size and max-messages must both be positive."
        )

    if args.skip_rows < 0:
        raise ValueError(
            "skip-rows must be non-negative."
        )

    start_row = load_start_row(
        progress_path=progress_path,
        input_path=input_path,
        topic=args.topic,
        requested_skip_rows=args.skip_rows,
    )

    logger.info(
        "Starting source data row position: %d",
        start_row,
    )

    rows_inspected = 0
    events_published = 0
    rows_skipped = 0
    rows_outside_period = 0
    stop_requested = False
    events_enqueued = 0
    checkpoint_position = start_row

    with input_path.open("r", encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file, delimiter=";")
        selected_columns = select_source_columns(reader.fieldnames)

        for _ in range(start_row):
            try:
                next(reader)
            except StopIteration:
                break

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            client_id="iot-realtime-pipeline-producer",
            acks="all",
            retries=5,
            linger_ms=20,
            compression_type="gzip",
        )

        try:
            for chunk_number, chunk in enumerate(
                iter_csv_batches(
                    reader=reader,
                    selected_columns=selected_columns,
                    chunk_size=args.chunk_size,
                ),
                start=1,
            ):
                pending_futures = []

                for row in chunk:
                    rows_inspected += 1
                    source_position = start_row + rows_inspected

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

                    except (TypeError, ValueError) as exc:
                        rows_skipped += 1

                        if rows_skipped <= 5:
                            logger.warning(
                                "Skipping source row %d: %s",
                                source_position,
                                exc,
                            )

                    else:
                        try:
                            value = json.dumps(
                                event,
                                allow_nan=False,
                                ensure_ascii=False,
                            ).encode("utf-8")
                        except Exception:
                            events_published += confirm_and_checkpoint_prefix(
                                producer=producer,
                                pending_futures=pending_futures,
                                progress_path=progress_path,
                                input_path=input_path,
                                topic=args.topic,
                                source_position=source_position,
                                last_checkpoint_position=checkpoint_position,
                            )
                            checkpoint_position = max(
                                checkpoint_position,
                                source_position - 1,
                            )
                            raise

                        try:
                            future = producer.send(
                                args.topic,
                                key=event["sensor_id"].encode("utf-8"),
                                value=value,
                            )
                        except Exception:
                            events_published += confirm_and_checkpoint_prefix(
                                producer=producer,
                                pending_futures=pending_futures,
                                progress_path=progress_path,
                                input_path=input_path,
                                topic=args.topic,
                                source_position=source_position,
                                last_checkpoint_position=checkpoint_position,
                            )
                            checkpoint_position = max(
                                checkpoint_position,
                                source_position - 1,
                            )
                            raise

                        pending_futures.append((source_position, future))
                        events_enqueued += 1

                        if events_enqueued >= args.max_messages:
                            stop_requested = True
                            break

                # Do not advance the checkpoint until every send from the
                # portion of this batch has been confirmed by Kafka.
                events_published += confirm_pending_sends(
                    producer=producer,
                    pending_futures=pending_futures,
                    progress_path=progress_path,
                    input_path=input_path,
                    topic=args.topic,
                    last_checkpoint_position=checkpoint_position,
                )

                rows_processed_total = start_row + rows_inspected
                save_progress(
                    progress_path=progress_path,
                    input_path=input_path,
                    topic=args.topic,
                    rows_processed=rows_processed_total,
                )
                checkpoint_position = rows_processed_total

                logger.info(
                    "Batch %d completed | inspected_this_run=%d "
                    "| source_rows_processed=%d | published=%d "
                    "| skipped=%d | outside_period=%d",
                    chunk_number,
                    rows_inspected,
                    rows_processed_total,
                    events_published,
                    rows_skipped,
                    rows_outside_period,
                )
                logger.info(
                    "Progress checkpoint saved | rows_processed=%d",
                    rows_processed_total,
                )

                if stop_requested:
                    break

        except KeyboardInterrupt:
            logger.warning(
                "Interrupted. The last source position not checkpointed may be "
                "republished if Kafka acknowledged it before interruption."
            )
            raise

        finally:
            producer.close()


    logger.info("Producer execution completed.")
    logger.info("Rows inspected this run: %d", rows_inspected)
    logger.info(
        "Source rows processed through: %d",
        start_row + rows_inspected,
    )
    logger.info("Events published: %d", events_published)
    logger.info("Rows skipped: %d", rows_skipped)
    logger.info(
        "Rows outside March 2026: %d",
        rows_outside_period,
    )
    logger.info("Topic: %s", args.topic)


if __name__ == "__main__":
    main()
