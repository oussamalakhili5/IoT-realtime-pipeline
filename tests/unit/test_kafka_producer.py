
"""Unit tests for the Kafka producer's source record validation."""

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.ingestion.kafka_producer import (
    OutsideReportingPeriodError,
    build_event_from_source_row,
    clean_text,
    load_start_row,
    main,
)

TEST_TEMP_ROOT = Path(__file__).resolve().parents[1]


class TestKafkaProducer(unittest.TestCase):
    """Test source record parsing and reporting period validation."""

    def setUp(self):
        self.valid_row = {
            "sensor_id": "30343",
            "timestamp": "2026-03-01 00:58:24",
            "pressure": "100850",
            "temperature": "8.0",
            "humidity": "42.0",
        }

    def test_accepts_record_inside_march(self):
        event = build_event_from_source_row(self.valid_row)

        self.assertEqual(event["sensor_id"], "30343")
        self.assertEqual(
            event["quality"]["pressure_status"],
            "valid_pa",
        )
        self.assertEqual(
            event["measurements"]["temperature_c"],
            8.0,
        )

    def test_source_missing_values_need_no_pandas(self):
        self.assertIsNone(clean_text(None))
        self.assertIsNone(clean_text(float("nan")))
        self.assertIsNone(clean_text(""))
        self.assertIsNone(clean_text("   "))
        self.assertIsNone(clean_text("NaN"))
        self.assertIsNone(clean_text("NULL"))
        self.assertNotIn("pandas", sys.modules)
        self.assertNotIn("pyarrow", sys.modules)

    def test_rejects_record_before_march(self):
        row = {
            **self.valid_row,
            "timestamp": "2026-02-20 08:29:01",
        }

        with self.assertRaises(OutsideReportingPeriodError):
            build_event_from_source_row(row)

    def test_rejects_record_at_start_of_april(self):
        row = {
            **self.valid_row,
            "timestamp": "2026-04-01 00:00:00",
        }

        with self.assertRaises(OutsideReportingPeriodError):
            build_event_from_source_row(row)

    def test_rejects_invalid_timestamp(self):
        row = {
            **self.valid_row,
            "timestamp": "invalid-timestamp",
        }

        with self.assertRaisesRegex(ValueError, "Invalid timestamp"):
            build_event_from_source_row(row)

    def test_removes_quotes_from_timestamp(self):
        row = {
            **self.valid_row,
            "timestamp": "'2026-03-01 00:58:24'",
        }

        event = build_event_from_source_row(row)

        self.assertEqual(
            event["timestamp"],
            "2026-03-01 00:58:24",
        )

    def test_accepts_iso_timestamp_with_t_separator(self):
        row = {**self.valid_row, "timestamp": "'2026-03-01T00:58:24'"}

        event = build_event_from_source_row(row)

        self.assertEqual(event["timestamp"], "2026-03-01 00:58:24")

    def test_normalizes_utc_z_and_offset_timestamps(self):
        timestamps = {
            "2026-03-01T23:59:32Z": "2026-03-01 23:59:32",
            "2026-03-01T23:59:32z": "2026-03-01 23:59:32",
            "2026-03-02T00:59:32+01:00": "2026-03-01 23:59:32",
            "2026-03-01T18:59:32-05:00": "2026-03-01 23:59:32",
        }

        for source_timestamp, expected in timestamps.items():
            with self.subTest(timestamp=source_timestamp):
                event = build_event_from_source_row({
                    **self.valid_row,
                    "timestamp": source_timestamp,
                })

                self.assertEqual(event["timestamp"], expected)

    def test_rejects_timestamp_with_invalid_calendar_date(self):
        row = {**self.valid_row, "timestamp": "2026-03-32 00:00:00"}

        with self.assertRaisesRegex(ValueError, "Invalid timestamp"):
            build_event_from_source_row(row)

    def _write_csv(self, path, timestamps):
        with path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file, delimiter=";")
            writer.writerow([
                "sensor_id", "location", "lat", "lon", "timestamp",
                "pressure", "temperature", "humidity",
            ])
            for index, timestamp in enumerate(timestamps, start=1):
                writer.writerow([
                    f"sensor-{index}", "lab", "49.9", "8.6", timestamp,
                    "100850", "8.0", "42.0",
                ])

    def _run_main(self, input_path, progress_path, *options):
        future = Mock()
        future.get.return_value = Mock()
        producer = Mock()
        producer.send.return_value = future

        arguments = [
            "kafka_producer",
            "--input", str(input_path),
            "--topic", "unit-test-topic",
            "--progress-file", str(progress_path),
            *options,
        ]
        with patch("sys.argv", arguments), patch(
            "src.ingestion.kafka_producer.KafkaProducer",
            return_value=producer,
        ):
            main()

        return producer, future

    def test_invalid_rows_before_valid_row_do_not_send_or_stop_validation(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(
                input_path,
                ["not-a-timestamp", "2026-03-01T00:00:01"],
            )

            producer, _ = self._run_main(
                input_path, progress_path, "--chunk-size", "2",
                "--max-messages", "1",
            )

            producer.send.assert_called_once()
            self.assertEqual(
                json.loads(
                    producer.send.call_args.kwargs["value"].decode("utf-8")
                )["timestamp"],
                "2026-03-01 00:00:01",
            )
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                2,
            )

    def test_max_messages_one_sends_at_most_one_valid_event(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(
                input_path,
                ["2026-03-01 00:00:01", "2026-03-01 00:00:02"],
            )

            producer, _ = self._run_main(
                input_path, progress_path, "--chunk-size", "2",
                "--max-messages", "1",
            )

            producer.send.assert_called_once()
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                1,
            )

    def test_max_messages_ten_sends_at_most_ten_events(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(
                input_path,
                [f"2026-03-01 00:00:{index:02d}" for index in range(1, 13)],
            )

            producer, _ = self._run_main(
                input_path, progress_path, "--chunk-size", "5",
                "--max-messages", "10",
            )

            self.assertEqual(producer.send.call_count, 10)
            self.assertEqual(producer.flush.call_count, 2)
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                10,
            )

    def test_csv_reader_handles_quoted_delimiters_and_trimmed_headers(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            with input_path.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.writer(csv_file, delimiter=";")
                writer.writerow([
                    " sensor_id ", " location ", " lat ", " lon ",
                    " timestamp ", " pressure ", " temperature ",
                    " humidity ",
                ])
                writer.writerow([
                    "sensor-1", "lab;west", "49.9", "8.6",
                    "2026-03-01 00:00:01", "100850", "8.0", "42.0",
                ])

            producer, _ = self._run_main(
                input_path,
                progress_path,
                "--max-messages", "1",
            )

            producer.send.assert_called_once()
            event = json.loads(
                producer.send.call_args.kwargs["value"].decode("utf-8")
            )
            self.assertEqual(event["location"]["location_id"], "lab;west")

    def test_missing_required_csv_column_fails_before_producer_creation(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            input_path.write_text(
                "sensor_id;location;lat;lon;timestamp;pressure;temperature\n",
                encoding="utf-8",
            )
            producer_factory = Mock()

            with patch("sys.argv", [
                "kafka_producer", "--input", str(input_path),
                "--topic", "unit-test-topic", "--progress-file",
                str(progress_path),
            ]), patch(
                "src.ingestion.kafka_producer.KafkaProducer",
                producer_factory,
            ):
                with self.assertRaisesRegex(ValueError, "Missing source columns"):
                    main()

            producer_factory.assert_not_called()

    def test_serialization_error_does_not_send_or_advance_checkpoint(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, ["2026-03-01 00:00:01"])
            producer = Mock()

            with patch("sys.argv", [
                "kafka_producer", "--input", str(input_path),
                "--topic", "unit-test-topic", "--progress-file",
                str(progress_path), "--max-messages", "1",
            ]), patch(
                "src.ingestion.kafka_producer.KafkaProducer",
                return_value=producer,
            ), patch(
                "src.ingestion.kafka_producer.build_event_from_source_row",
                return_value={"sensor_id": "sensor-1", "bad": object()},
            ):
                with self.assertRaises(TypeError):
                    main()

            producer.send.assert_not_called()
            self.assertFalse(progress_path.exists())

    def test_warning_limit_does_not_change_invalid_row_handling(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, [
                *(["not-a-timestamp"] * 7),
                "2026-03-01T00:00:01",
            ])

            with self.assertLogs(
                "src.ingestion.kafka_producer", level="WARNING"
            ) as logs:
                producer, _ = self._run_main(
                    input_path,
                    progress_path,
                    "--chunk-size", "8",
                    "--max-messages", "1",
                )

            self.assertEqual(len(logs.records), 5)
            producer.send.assert_called_once()
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                8,
            )

    def test_invalid_rows_after_valid_event_never_republish_previous_event(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, [
                "2026-03-01 00:00:01",
                *("not-a-timestamp" for _ in range(7)),
            ])

            producer, _ = self._run_main(
                input_path,
                progress_path,
                "--chunk-size", "8",
                "--max-messages", "10",
            )

            producer.send.assert_called_once()
            published = json.loads(
                producer.send.call_args.kwargs["value"].decode("utf-8")
            )
            self.assertEqual(published["timestamp"], "2026-03-01 00:00:01")
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                8,
            )

    def test_checkpoint_is_saved_only_after_send_confirmation(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, ["2026-03-01 00:00:01"])

            acknowledged = []
            future = Mock()
            future.get.side_effect = lambda timeout: acknowledged.append(True)
            producer = Mock()
            producer.send.return_value = future
            original_save = __import__(
                "src.ingestion.kafka_producer", fromlist=["save_progress"]
            ).save_progress

            def check_ack_before_save(*args, **kwargs):
                self.assertTrue(acknowledged)
                return original_save(*args, **kwargs)

            with patch("sys.argv", [
                "kafka_producer", "--input", str(input_path),
                "--topic", "unit-test-topic", "--progress-file",
                str(progress_path), "--max-messages", "1",
            ]), patch(
                "src.ingestion.kafka_producer.KafkaProducer",
                return_value=producer,
            ), patch(
                "src.ingestion.kafka_producer.save_progress",
                side_effect=check_ack_before_save,
            ):
                main()

            self.assertTrue(progress_path.is_file())

    def test_failed_confirmation_preserves_previous_checkpoint(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, ["2026-03-01 00:00:01"])
            old_checkpoint = {
                "input_file": str(input_path.resolve()),
                "topic": "unit-test-topic",
                "rows_processed": 0,
            }
            progress_path.write_text(json.dumps(old_checkpoint), encoding="utf-8")
            future = Mock()
            future.get.side_effect = TimeoutError("not acknowledged")
            producer = Mock()
            producer.send.return_value = future

            with patch("sys.argv", [
                "kafka_producer", "--input", str(input_path),
                "--topic", "unit-test-topic", "--progress-file",
                str(progress_path), "--max-messages", "1",
            ]), patch(
                "src.ingestion.kafka_producer.KafkaProducer",
                return_value=producer,
            ):
                with self.assertRaises(TimeoutError):
                    main()

            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8")),
                old_checkpoint,
            )

    def test_failed_confirmation_checkpoints_only_the_confirmed_source_prefix(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            self._write_csv(input_path, [
                "2026-03-01 00:00:01",
                "not-a-timestamp",
                "2026-03-01 00:00:03",
            ])

            first_future = Mock()
            failed_future = Mock()
            failed_future.get.side_effect = TimeoutError("not acknowledged")
            producer = Mock()
            producer.send.side_effect = [first_future, failed_future]

            with patch("sys.argv", [
                "kafka_producer", "--input", str(input_path),
                "--topic", "unit-test-topic", "--progress-file",
                str(progress_path), "--chunk-size", "3",
            ]), patch(
                "src.ingestion.kafka_producer.KafkaProducer",
                return_value=producer,
            ):
                with self.assertRaises(TimeoutError):
                    main()

            self.assertEqual(first_future.get.call_count, 1)
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                2,
            )

            resumed_producer, _ = self._run_main(
                input_path,
                progress_path,
                "--max-messages", "1",
            )
            resumed_producer.send.assert_called_once()
            resumed_event = json.loads(
                resumed_producer.send.call_args.kwargs["value"].decode("utf-8")
            )
            self.assertEqual(resumed_event["timestamp"], "2026-03-01 00:00:03")
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                3,
            )

    def test_rejects_malformed_checkpoint(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            progress_path = root / "progress.json"
            progress_path.write_text("{bad json", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Cannot read"):
                load_start_row(progress_path, root / "events.csv", "topic", 0)

    def test_rejects_checkpoint_for_different_file_or_topic(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "progress.json"
            progress_path.write_text(json.dumps({
                "input_file": str(input_path),
                "topic": "topic-a",
                "rows_processed": 2,
            }), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "different Kafka topic"):
                load_start_row(progress_path, input_path, "topic-b", 0)
            with self.assertRaisesRegex(ValueError, "different input file"):
                load_start_row(progress_path, root / "other.csv", "topic-a", 0)

    def test_resumes_from_checkpoint_and_uses_chunk_skip_and_progress_options(self):
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as directory:
            root = Path(directory)
            input_path = root / "events.csv"
            progress_path = root / "custom-progress.json"
            self._write_csv(input_path, [
                "2026-03-01 00:00:01",
                "2026-03-01T00:00:02",
                "2026-03-01 00:00:03",
            ])
            progress_path.write_text(json.dumps({
                "input_file": str(input_path.resolve()),
                "topic": "unit-test-topic",
                "rows_processed": 1,
            }), encoding="utf-8")

            producer, _ = self._run_main(
                input_path,
                progress_path,
                "--chunk-size", "2",
                "--max-messages", "1",
                "--skip-rows", "1",
            )

            producer.send.assert_called_once()
            self.assertEqual(
                json.loads(
                    producer.send.call_args.kwargs["value"].decode("utf-8")
                )["timestamp"],
                "2026-03-01 00:00:02",
            )
            self.assertEqual(
                json.loads(progress_path.read_text(encoding="utf-8"))[
                    "rows_processed"
                ],
                2,
            )


if __name__ == "__main__":
    unittest.main()
