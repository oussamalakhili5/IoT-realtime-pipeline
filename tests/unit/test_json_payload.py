"""Regression tests for leading UTF-8 BOM normalization."""

from datetime import datetime, timedelta
import json
import re
import unittest
from unittest.mock import patch

from src.streaming.anomaly_rules import classify_anomaly_status
from src.streaming.json_payload import (
    LEADING_UTF8_BOM_PATTERN,
    UTF8_BOM,
    normalize_json_payload,
    normalize_json_payload_column,
)
from src.streaming.measurement_rules import assess_measurement


def make_event(schema_version="1.1", sensor_id="anomaly-test-sensor-001"):
    return {
        "schema_version": schema_version,
        "sensor_id": sensor_id,
        "timestamp": "2026-03-02 00:00:00",
        "location": {
            "location_id": "test-lab",
            "latitude": 49.9,
            "longitude": 8.6,
        },
        "measurements": {
            "pressure_raw": "100850",
            "pressure_numeric": 100850.0,
            "pressure_pa_candidate": 100850.0,
            "pressure_unit_candidate": "Pa",
            "temperature_c": 8.0,
            "humidity_pct": 42.0,
        },
        "quality": {
            "pressure_status": "valid_pa",
            "temperature_status": "valid_temperature",
            "humidity_status": "valid_humidity",
            "humidity_at_boundary": False,
            "record_status": "accepted_candidate",
        },
    }


class TestJsonPayloadNormalization(unittest.TestCase):
    """Check BOM semantics and the supplied anomaly observations."""

    def test_valid_json_without_bom_parses_unchanged(self):
        payload = json.dumps(make_event())

        self.assertEqual(normalize_json_payload(payload), payload)
        self.assertEqual(json.loads(normalize_json_payload(payload)), make_event())

    def test_leading_bom_and_plain_json_have_identical_event_content(self):
        payload = json.dumps(make_event())
        prefixed_payload = UTF8_BOM + payload

        plain = json.loads(normalize_json_payload(payload))
        prefixed = json.loads(normalize_json_payload(prefixed_payload))

        self.assertEqual(plain, prefixed)
        self.assertEqual(prefixed["schema_version"], "1.1")
        self.assertEqual(
            prefixed["quality"]["record_status"],
            "accepted_candidate",
        )

    def test_invalid_json_is_not_made_valid_by_normalization(self):
        invalid_payload = UTF8_BOM + "{not-json"
        normalized = normalize_json_payload(invalid_payload)

        self.assertEqual(normalized, "{not-json")
        with self.assertRaises(json.JSONDecodeError):
            json.loads(normalized)

    def test_unsupported_schema_is_preserved_for_existing_classification(self):
        payload = json.dumps(make_event(schema_version="9.9"))
        parsed = json.loads(normalize_json_payload(UTF8_BOM + payload))

        self.assertEqual(parsed["schema_version"], "9.9")
        self.assertNotIn(parsed["schema_version"], {"1.0", "1.1"})

    def test_bom_inside_json_string_is_preserved(self):
        sensor_id = f"anomaly-{UTF8_BOM}sensor-001"
        payload = json.dumps(make_event(sensor_id=sensor_id))
        parsed = json.loads(normalize_json_payload(payload))

        self.assertEqual(parsed["sensor_id"], sensor_id)
        self.assertIn(UTF8_BOM, parsed["sensor_id"])

    def test_spark_pattern_is_anchored_to_the_first_character(self):
        self.assertEqual(LEADING_UTF8_BOM_PATTERN, f"^{UTF8_BOM}")
        self.assertEqual(
            re.sub(LEADING_UTF8_BOM_PATTERN, "", f"{UTF8_BOM}{{}}"),
            "{}",
        )
        self.assertEqual(
            re.sub(LEADING_UTF8_BOM_PATTERN, "", f"{{}}{UTF8_BOM}"),
            f"{{}}{UTF8_BOM}",
        )

    def test_spark_column_helper_uses_the_leading_only_pattern(self):
        marker = object()
        payload_column = object()

        with patch(
            "pyspark.sql.functions.regexp_replace",
            return_value=marker,
        ) as spark_regexp_replace:
            result = normalize_json_payload_column(payload_column)

        self.assertIs(result, marker)
        spark_regexp_replace.assert_called_once_with(
            payload_column,
            LEADING_UTF8_BOM_PATTERN,
            "",
        )

    def test_anomaly_observations_are_quality_accepted_and_exceed_thresholds(self):
        observations = [
            (100850.0, 8.0, 42.0),
            (100900.0, 9.0, 45.0),
            (102100.0, 15.0, 70.0),
        ]
        assessments = [
            assess_measurement(pressure, temperature, humidity)
            for pressure, temperature, humidity in observations
        ]

        self.assertEqual(
            [assessment["record_status"] for assessment in assessments],
            ["accepted_candidate"] * 3,
        )

        pressure_range = max(item[0] for item in observations) - min(
            item[0] for item in observations
        )
        temperature_range = max(item[1] for item in observations) - min(
            item[1] for item in observations
        )
        humidity_range = max(item[2] for item in observations) - min(
            item[2] for item in observations
        )

        self.assertEqual(
            (pressure_range, temperature_range, humidity_range),
            (1250.0, 7.0, 28.0),
        )
        self.assertEqual(
            classify_anomaly_status(
                3,
                3,
                3,
                pressure_range,
                temperature_range,
                humidity_range,
            ),
            "anomaly_suspected",
        )

        timestamps = [
            datetime(2026, 3, 2, 0, minute)
            for minute in (0, 1, 2)
        ]
        window_start = datetime(2026, 3, 2, 0, 0)
        window_end = window_start + timedelta(minutes=10)
        self.assertTrue(
            all(window_start <= timestamp < window_end for timestamp in timestamps)
        )


if __name__ == "__main__":
    unittest.main()
