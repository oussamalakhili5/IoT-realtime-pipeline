"""Environment-backed configuration for the Spark Kafka consumer."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


DEFAULT_KAFKA_TOPIC = "iot-sensor-events-march-pipeline"
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 100
DEFAULT_ROUTING_CHECKPOINT_RELATIVE_PATH = Path(
    "data/interim/checkpoints/spark_iot_kafka_march_pg_routing_v1"
)
DEFAULT_WINDOW_CHECKPOINT_RELATIVE_PATH = Path(
    "data/interim/checkpoints/spark_iot_kafka_march_silo_windows_v1"
)

IOT_KAFKA_TOPIC_ENV = "IOT_KAFKA_TOPIC"
IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV = "IOT_SPARK_MAX_OFFSETS_PER_TRIGGER"
IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV = "IOT_SPARK_EVENT_PERSISTENCE_MODE"
IOT_SPARK_EVENT_SILO_RUN_ID_ENV = "IOT_SPARK_EVENT_SILO_RUN_ID"
IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV = "IOT_SPARK_WINDOW_PERSISTENCE_MODE"
IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV = "IOT_SPARK_ROUTING_CHECKPOINT_DIR"
IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV = "IOT_SPARK_WINDOW_CHECKPOINT_DIR"

SILO_WINDOW_METRICS_ROOT = "s3a://iot-data-lake/processed/window_metrics"
SILO_EVENT_RECORDS_ROOT = "s3a://iot-data-lake/processed/event_records"
DEFAULT_EVENT_PERSISTENCE_MODE = "postgres"
EVENT_PERSISTENCE_MODES = frozenset(
    {"postgres", "silo"}
)
DEFAULT_WINDOW_PERSISTENCE_MODE = "postgres"
WINDOW_PERSISTENCE_MODES = frozenset(
    {"postgres", "silo_summaries"}
)
_SAFE_SILO_RUN_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SAFE_KAFKA_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def _validate_topic(topic: str) -> None:
    """Reject topic names that Kafka would reject or that split S3 paths."""
    if (
        not isinstance(topic, str)
        or topic in {".", ".."}
        or not _SAFE_KAFKA_TOPIC.fullmatch(topic)
    ):
        raise ValueError(
            f"{IOT_KAFKA_TOPIC_ENV} must be a valid Kafka topic name "
            "(1 to 249 ASCII letters, digits, dots, underscores, or hyphens; "
            "'.' and '..' are not allowed)."
        )


def _validate_run_id(run_id: str) -> None:
    if (
        not isinstance(run_id, str)
        or not _SAFE_SILO_RUN_ID.fullmatch(run_id)
    ):
        raise ValueError("run_id must be a safe 1-to-64-character path token.")


def _validate_batch_id(batch_id: int) -> None:
    if (
        isinstance(batch_id, bool)
        or not isinstance(batch_id, int)
        or batch_id < 0
    ):
        raise ValueError("batch_id must be a non-negative integer.")


@dataclass(frozen=True)
class KafkaConsumerConfig:
    """Resolved topic and checkpoint paths for one consumer process."""

    topic: str
    max_offsets_per_trigger: int
    event_persistence_mode: str
    event_silo_run_id: str | None
    window_persistence_mode: str
    routing_checkpoint_dir: Path
    window_checkpoint_dir: Path


def _checkpoint_path(
    value: str | None,
    default_relative_path: Path,
    project_root: Path,
) -> Path:
    """Resolve a checkpoint path, anchoring configured relative paths."""
    if value is None or not value.strip():
        return project_root / default_relative_path

    path = Path(value.strip()).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def resolve_consumer_config(
    project_root: Path,
    environ: Mapping[str, str] | None = None,
) -> KafkaConsumerConfig:
    """Resolve consumer settings without importing or starting Spark."""
    root = Path(project_root).resolve(strict=False)
    environment = os.environ if environ is None else environ

    topic = environment.get(IOT_KAFKA_TOPIC_ENV, "").strip()
    if not topic:
        topic = DEFAULT_KAFKA_TOPIC
    _validate_topic(topic)

    configured_max_offsets = environment.get(
        IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV,
        "",
    ).strip()
    if configured_max_offsets:
        try:
            max_offsets_per_trigger = int(configured_max_offsets)
        except ValueError as exc:
            raise ValueError(
                f"{IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV} must be a positive integer."
            ) from exc
        if max_offsets_per_trigger < 1:
            raise ValueError(
                f"{IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV} must be a positive integer."
            )
    else:
        max_offsets_per_trigger = DEFAULT_MAX_OFFSETS_PER_TRIGGER

    event_persistence_mode = environment.get(
        IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV,
        DEFAULT_EVENT_PERSISTENCE_MODE,
    ).strip().lower()
    if event_persistence_mode not in EVENT_PERSISTENCE_MODES:
        allowed_modes = ", ".join(sorted(EVENT_PERSISTENCE_MODES))
        raise ValueError(
            f"{IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV} must be one of: "
            f"{allowed_modes}."
        )

    event_silo_run_id = environment.get(
        IOT_SPARK_EVENT_SILO_RUN_ID_ENV,
        "",
    ).strip() or None
    if event_silo_run_id and not _SAFE_SILO_RUN_ID.fullmatch(
        event_silo_run_id
    ):
        raise ValueError(
            f"{IOT_SPARK_EVENT_SILO_RUN_ID_ENV} must contain only "
            "letters, numbers, dots, underscores, and hyphens "
            "(1 to 64 characters)."
        )
    if (
        event_persistence_mode == "silo"
        and event_silo_run_id is None
    ):
        raise ValueError(
            f"{IOT_SPARK_EVENT_SILO_RUN_ID_ENV} is required when "
            f"{IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV} writes event Parquet."
        )

    window_persistence_mode = environment.get(
        IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV,
        DEFAULT_WINDOW_PERSISTENCE_MODE,
    ).strip().lower()
    if window_persistence_mode not in WINDOW_PERSISTENCE_MODES:
        allowed_modes = ", ".join(sorted(WINDOW_PERSISTENCE_MODES))
        raise ValueError(
            f"{IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV} must be one of: "
            f"{allowed_modes}."
        )

    if window_persistence_mode == "silo_summaries":
        if event_persistence_mode != "silo" or event_silo_run_id is None:
            raise ValueError(
                f"{IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV}=silo_summaries "
                "requires event persistence mode 'silo' and a dedicated "
                f"{IOT_SPARK_EVENT_SILO_RUN_ID_ENV}."
            )
        for variable_name in (
            IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV,
            IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV,
        ):
            if not environment.get(variable_name, "").strip():
                raise ValueError(
                    f"{variable_name} must point to a dedicated checkpoint "
                    "when window persistence mode is silo_summaries."
                )

    routing_checkpoint_dir = _checkpoint_path(
        environment.get(IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV),
        DEFAULT_ROUTING_CHECKPOINT_RELATIVE_PATH,
        root,
    )
    window_checkpoint_dir = _checkpoint_path(
        environment.get(IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV),
        DEFAULT_WINDOW_CHECKPOINT_RELATIVE_PATH,
        root,
    )
    if window_persistence_mode == "silo_summaries":
        if (
            routing_checkpoint_dir
            == root / DEFAULT_ROUTING_CHECKPOINT_RELATIVE_PATH
            or window_checkpoint_dir
            == root / DEFAULT_WINDOW_CHECKPOINT_RELATIVE_PATH
            or routing_checkpoint_dir == window_checkpoint_dir
        ):
            raise ValueError(
                "silo_summaries requires separate checkpoint paths outside "
                "the historical defaults."
            )

    return KafkaConsumerConfig(
        topic=topic,
        max_offsets_per_trigger=max_offsets_per_trigger,
        event_persistence_mode=event_persistence_mode,
        event_silo_run_id=event_silo_run_id,
        window_persistence_mode=window_persistence_mode,
        routing_checkpoint_dir=routing_checkpoint_dir,
        window_checkpoint_dir=window_checkpoint_dir,
    )


def silo_window_batch_path(topic: str, batch_id: int) -> str:
    """Return the topic-scoped Silo prefix for a metrics micro-batch."""
    _validate_topic(topic)
    _validate_batch_id(batch_id)
    return (
        f"{SILO_WINDOW_METRICS_ROOT}/topic={topic}/"
        f"batch_id={batch_id:020d}"
    )


def silo_window_run_batch_path(
    topic: str,
    run_id: str,
    batch_id: int,
) -> str:
    """Return an isolated run- and batch-scoped metrics prefix."""
    _validate_topic(topic)
    _validate_run_id(run_id)
    _validate_batch_id(batch_id)
    return (
        f"{SILO_WINDOW_METRICS_ROOT}/topic={topic}/run_id={run_id}/"
        f"batch_id={batch_id:020d}"
    )


def silo_event_batch_path(
    topic: str,
    run_id: str,
    batch_id: int,
) -> str:
    """Return a run- and batch-scoped prefix for classified event Parquet."""
    _validate_topic(topic)
    _validate_run_id(run_id)
    _validate_batch_id(batch_id)
    return (
        f"{SILO_EVENT_RECORDS_ROOT}/topic={topic}/run_id={run_id}/"
        f"batch_id={batch_id:020d}"
    )
