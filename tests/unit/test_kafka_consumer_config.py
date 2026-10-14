"""Tests for Spark consumer configuration without starting Spark."""

import sys
import subprocess
import unittest
from pathlib import Path

from src.streaming.kafka_consumer_config import (
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_MAX_OFFSETS_PER_TRIGGER,
    DEFAULT_EVENT_PERSISTENCE_MODE,
    DEFAULT_WINDOW_PERSISTENCE_MODE,
    IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV,
    IOT_SPARK_EVENT_SILO_RUN_ID_ENV,
    IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV,
    IOT_KAFKA_TOPIC_ENV,
    IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV,
    IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV,
    IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV,
    resolve_consumer_config,
    silo_event_batch_path,
    silo_window_batch_path,
    silo_window_run_batch_path,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestKafkaConsumerConfig(unittest.TestCase):
    """Cover defaults and isolated-test overrides without Spark imports."""

    def test_configuration_module_does_not_import_spark(self):
        probe = (
            "import sys; "
            "import src.streaming.kafka_consumer_config; "
            "assert not any(name == 'pyspark' or name.startswith('pyspark.') "
            "for name in sys.modules)"
        )
        subprocess.run(
            [sys.executable, "-c", probe],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_defaults_preserve_historical_topic_and_checkpoints(self):
        config = resolve_consumer_config(PROJECT_ROOT, environ={})

        self.assertEqual(config.topic, "iot-sensor-events-march-pipeline")
        self.assertEqual(config.topic, DEFAULT_KAFKA_TOPIC)
        self.assertEqual(
            config.event_persistence_mode,
            DEFAULT_EVENT_PERSISTENCE_MODE,
        )
        self.assertEqual(config.event_silo_run_id, None)
        self.assertEqual(
            config.window_persistence_mode,
            DEFAULT_WINDOW_PERSISTENCE_MODE,
        )
        self.assertEqual(
            config.max_offsets_per_trigger,
            DEFAULT_MAX_OFFSETS_PER_TRIGGER,
        )
        self.assertEqual(
            config.routing_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/spark_iot_kafka_march_pg_routing_v1",
        )
        self.assertEqual(
            config.window_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/spark_iot_kafka_march_silo_windows_v1",
        )

    def test_clean_topic_and_checkpoints_are_resolved_from_environment(self):
        config = resolve_consumer_config(
            PROJECT_ROOT,
            environ={
                IOT_KAFKA_TOPIC_ENV:
                    "iot-sensor-events-march-pipeline-clean-v1",
                IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV:
                    "data/interim/checkpoints/"
                    "spark_iot_kafka_march_pg_routing_clean_v1",
                IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV:
                    "data/interim/checkpoints/"
                    "spark_iot_kafka_march_silo_windows_clean_v1",
            },
        )

        self.assertEqual(
            config.topic,
            "iot-sensor-events-march-pipeline-clean-v1",
        )
        self.assertEqual(
            config.routing_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/"
            "spark_iot_kafka_march_pg_routing_clean_v1",
        )
        self.assertEqual(
            config.window_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/"
            "spark_iot_kafka_march_silo_windows_clean_v1",
        )

    def test_empty_environment_values_fall_back_to_historical_defaults(self):
        config = resolve_consumer_config(
            PROJECT_ROOT,
            environ={
                IOT_KAFKA_TOPIC_ENV: " ",
                IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV: "",
                IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV: "  ",
            },
        )

        self.assertEqual(config.topic, DEFAULT_KAFKA_TOPIC)
        self.assertEqual(
            config.max_offsets_per_trigger,
            DEFAULT_MAX_OFFSETS_PER_TRIGGER,
        )
        self.assertEqual(
            config.routing_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/spark_iot_kafka_march_pg_routing_v1",
        )
        self.assertEqual(
            config.window_checkpoint_dir,
            PROJECT_ROOT
            / "data/interim/checkpoints/spark_iot_kafka_march_silo_windows_v1",
        )

    def test_topic_names_are_validated_before_path_or_broker_use(self):
        for value in ("unsafe/topic", "topic with spaces", ".", "..", "x" * 250):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "valid Kafka topic"):
                    resolve_consumer_config(
                        PROJECT_ROOT,
                        environ={IOT_KAFKA_TOPIC_ENV: value},
                    )

    def test_max_offsets_per_trigger_can_be_configured(self):
        config = resolve_consumer_config(
            PROJECT_ROOT,
            environ={IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV: "10000"},
        )

        self.assertEqual(config.max_offsets_per_trigger, 10000)

    def test_max_offsets_per_trigger_must_be_positive_integer(self):
        for value in ("0", "-1", "not-an-integer"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    resolve_consumer_config(
                        PROJECT_ROOT,
                        environ={IOT_SPARK_MAX_OFFSETS_PER_TRIGGER_ENV: value},
                    )

    def test_silo_paths_are_separate_per_topic(self):
        historical_path = silo_window_batch_path(
            "iot-sensor-events-march-pipeline",
            12,
        )
        clean_path = silo_window_batch_path(
            "iot-sensor-events-march-pipeline-clean-v1",
            12,
        )

        self.assertEqual(
            historical_path,
            "s3a://iot-data-lake/processed/window_metrics/"
            "topic=iot-sensor-events-march-pipeline/"
            "batch_id=00000000000000000012",
        )
        self.assertNotEqual(clean_path, historical_path)
        self.assertIn(
            "/topic=iot-sensor-events-march-pipeline-clean-v1/",
            clean_path,
        )

    def test_event_silo_mode_requires_and_resolves_a_run_id(self):
        config = resolve_consumer_config(
            PROJECT_ROOT,
            environ={
                IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo",
                IOT_SPARK_EVENT_SILO_RUN_ID_ENV: "march-full-v1",
            },
        )

        self.assertEqual(config.event_persistence_mode, "silo")
        self.assertEqual(config.event_silo_run_id, "march-full-v1")

        with self.assertRaisesRegex(ValueError, "is required"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo"},
            )

    def test_event_persistence_mode_and_run_id_are_validated(self):
        with self.assertRaisesRegex(ValueError, "must be one of"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "database"},
            )

        with self.assertRaisesRegex(ValueError, "must contain only"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={
                    IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo",
                    IOT_SPARK_EVENT_SILO_RUN_ID_ENV: "unsafe/run",
                },
            )

    def test_event_paths_are_isolated_by_run_and_batch(self):
        first_batch_retry = silo_event_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            7,
        )
        same_batch_retry = silo_event_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            7,
        )
        next_batch = silo_event_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            8,
        )
        new_run = silo_event_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1-replay",
            7,
        )

        self.assertEqual(first_batch_retry, same_batch_retry)
        self.assertNotEqual(first_batch_retry, next_batch)
        self.assertNotEqual(first_batch_retry, new_run)
        self.assertEqual(
            first_batch_retry,
            "s3a://iot-data-lake/processed/event_records/"
            "topic=iot-sensor-events-march-full-v1/"
            "run_id=march-full-v1/batch_id=00000000000000000007",
        )

    def test_silo_paths_reject_unsafe_topics_run_ids_and_batch_ids(self):
        with self.assertRaisesRegex(ValueError, "valid Kafka topic"):
            silo_event_batch_path("bad/topic", "run-1", 0)
        with self.assertRaisesRegex(ValueError, "safe 1-to-64-character"):
            silo_event_batch_path("valid-topic", "../run", 0)
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            silo_window_batch_path("valid-topic", -1)

    def test_window_summary_mode_requires_silo_and_dedicated_checkpoints(self):
        with self.assertRaisesRegex(ValueError, "requires event persistence"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={
                    IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV: "silo_summaries",
                },
            )

        with self.assertRaisesRegex(ValueError, "dedicated checkpoint"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={
                    IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV: "silo_summaries",
                    IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo",
                    IOT_SPARK_EVENT_SILO_RUN_ID_ENV: "test-run",
                },
            )

    def test_window_summary_mode_keeps_run_and_checkpoint_scope_explicit(self):
        config = resolve_consumer_config(
            PROJECT_ROOT,
            environ={
                IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo",
                IOT_SPARK_EVENT_SILO_RUN_ID_ENV: "march-full-v1",
                IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV: "silo_summaries",
                IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV:
                    "data/interim/checkpoints/routing-full-v1",
                IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV:
                    "data/interim/checkpoints/windows-full-v1",
            },
        )

        self.assertEqual(config.window_persistence_mode, "silo_summaries")
        self.assertEqual(config.event_silo_run_id, "march-full-v1")
        self.assertEqual(
            config.routing_checkpoint_dir,
            PROJECT_ROOT / "data/interim/checkpoints/routing-full-v1",
        )
        self.assertEqual(
            config.window_checkpoint_dir,
            PROJECT_ROOT / "data/interim/checkpoints/windows-full-v1",
        )

        with self.assertRaisesRegex(ValueError, "outside the historical defaults"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={
                    IOT_SPARK_EVENT_PERSISTENCE_MODE_ENV: "silo",
                    IOT_SPARK_EVENT_SILO_RUN_ID_ENV: "march-full-v1",
                    IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV: "silo_summaries",
                    IOT_SPARK_ROUTING_CHECKPOINT_DIR_ENV:
                        "data/interim/checkpoints/spark_iot_kafka_march_pg_routing_v1",
                    IOT_SPARK_WINDOW_CHECKPOINT_DIR_ENV:
                        "data/interim/checkpoints/windows-full-v1",
                },
            )

    def test_window_persistence_mode_rejects_unknown_values(self):
        with self.assertRaisesRegex(ValueError, "must be one of"):
            resolve_consumer_config(
                PROJECT_ROOT,
                environ={IOT_SPARK_WINDOW_PERSISTENCE_MODE_ENV: "silo"},
            )

    def test_window_silo_paths_are_isolated_by_run_and_batch(self):
        first = silo_window_run_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            7,
        )
        retry = silo_window_run_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            7,
        )
        next_batch = silo_window_run_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1",
            8,
        )
        next_run = silo_window_run_batch_path(
            "iot-sensor-events-march-full-v1",
            "march-full-v1-replay",
            7,
        )

        self.assertEqual(first, retry)
        self.assertNotEqual(first, next_batch)
        self.assertNotEqual(first, next_run)
        self.assertIn("/run_id=march-full-v1/", first)


if __name__ == "__main__":
    unittest.main()
