
"""Unit tests for the Kafka producer's source record validation."""

import unittest

from src.ingestion.kafka_producer import (
    OutsideReportingPeriodError,
    build_event_from_source_row,
)


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


if __name__ == "__main__":
    unittest.main()
