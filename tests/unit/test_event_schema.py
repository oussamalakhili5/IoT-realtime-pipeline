
"""Unit tests for canonical IoT event construction."""

import json
import unittest

from src.streaming.event_schema import (
    SCHEMA_VERSION,
    build_event,
    validate_event,
)


def make_sample_row(**overrides):
    """Return a representative annotated sensor record."""
    row = {
        "sensor_id": 39287,
        "timestamp": "2026-03-01T00:01:55",
        "pressure_raw": "1002.0",
        "pressure_numeric": 1002.0,
        "pressure_pa_candidate": 100200.0,
        "pressure_unit_candidate": "hPa_candidate",
        "pressure_status": "converted_from_hpa",
        "temperature_value": 25.9,
        "temperature_status": "valid_temperature",
        "humidity_value": 93.0,
        "humidity_status": "valid_humidity",
        "humidity_at_boundary": False,
        "record_status": "accepted_candidate",
    }

    row.update(overrides)
    return row


class TestEventSchema(unittest.TestCase):
    """Test canonical IoT event construction and validation."""

    def test_builds_expected_event_structure(self):
        event = build_event(make_sample_row())

        self.assertEqual(event["schema_version"], SCHEMA_VERSION)
        self.assertEqual(event["sensor_id"], "39287")
        self.assertEqual(event["timestamp"], "2026-03-01T00:01:55")
        self.assertEqual(
            event["measurements"]["pressure_pa_candidate"],
            100200.0,
        )
        self.assertEqual(
            event["quality"]["pressure_status"],
            "converted_from_hpa",
        )

    def test_preserves_missing_numeric_values_as_none(self):
        event = build_event(
            make_sample_row(
                pressure_numeric=float("nan"),
                pressure_pa_candidate=float("nan"),
                temperature_value=None,
                humidity_value=None,
            )
        )

        measurements = event["measurements"]

        self.assertIsNone(measurements["pressure_numeric"])
        self.assertIsNone(measurements["pressure_pa_candidate"])
        self.assertIsNone(measurements["temperature_c"])
        self.assertIsNone(measurements["humidity_pct"])

    def test_requires_sensor_identifier(self):
        with self.assertRaises(ValueError):
            build_event(make_sample_row(sensor_id=None))

    def test_requires_quality_statuses(self):
        with self.assertRaises(ValueError):
            build_event(make_sample_row(record_status=None))

    def test_event_is_serializable_as_standard_json(self):
        event = build_event(make_sample_row())

        serialized = json.dumps(event, allow_nan=False)
        decoded = json.loads(serialized)

        self.assertEqual(decoded, event)

    def test_rejects_missing_event_fields(self):
        with self.assertRaises(ValueError):
            validate_event({"schema_version": "1.0"})


if __name__ == "__main__":
    unittest.main()
