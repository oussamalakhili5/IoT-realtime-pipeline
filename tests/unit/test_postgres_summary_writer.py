"""Unit coverage for idempotent PostgreSQL summary upserts."""

import re
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from src.storage.postgres_writer import (
    _location_key,
    persist_event_summary_batch,
    persist_window_summary_batch,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.claim_result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.connection.executions.append((statement, params))
        if "INSERT INTO iot_stream_batch_progress" in statement:
            topic, run_id, sink_name, batch_id = params
            key = (topic, run_id, sink_name)
            previous = self.connection.progress.get(key)
            if previous is None or batch_id > previous:
                self.connection.progress[key] = batch_id
                self.claim_result = (batch_id,)
            else:
                self.claim_result = None

    def fetchone(self):
        return self.claim_result


class FakeConnection:
    def __init__(self):
        self.progress = {}
        self.executions = []
        self.close_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.close_count += 1


class TestPostgresSummaryWriter(unittest.TestCase):
    def assert_insert_arity(self, statement, params):
        match = re.search(
            r"INSERT INTO\s+\w+\s*\((.*?)\)\s*VALUES\s*\((.*?)\)",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        self.assertIsNotNone(match)
        columns = [column.strip() for column in match.group(1).split(",")]
        placeholders = match.group(2).count("%s")
        self.assertEqual(len(columns), placeholders)
        self.assertEqual(placeholders, len(params))

    def test_location_key_is_stable_without_id_and_with_nonfinite_coords(self):
        row = {
            "location_id": None,
            "latitude": float("nan"),
            "longitude": float("inf"),
        }

        key = _location_key(row)

        self.assertEqual(key, _location_key(dict(row)))
        self.assertRegex(key, r"^loc1:[0-9a-f]{64}$")
        self.assertNotEqual(
            key,
            _location_key(
                {
                    "location_id": None,
                    "latitude": float("nan"),
                    "longitude": float("-inf"),
                }
            ),
        )
        self.assertEqual(
            _location_key(
                {"location_id": None, "latitude": -0.0, "longitude": 2}
            ),
            _location_key(
                {"location_id": None, "latitude": 0.0, "longitude": 2.0}
            ),
        )

    def test_event_summaries_are_written_once_when_spark_retries_batch(self):
        connection = FakeConnection()
        hourly = {
            "hour_start": datetime(2026, 3, 1, 0, 0),
            "hour_end": datetime(2026, 3, 1, 1, 0),
            "accepted_event_count": 2,
            "pressure_observation_count": 2,
            "pressure_sum": 201700.0,
            "pressure_min": 100800.0,
            "pressure_max": 100900.0,
            "temperature_observation_count": 2,
            "temperature_sum": 16.0,
            "temperature_min": 7.0,
            "temperature_max": 9.0,
            "humidity_observation_count": 2,
            "humidity_sum": 84.0,
            "humidity_min": 42.0,
            "humidity_max": 42.0,
        }
        daily = {
            "summary_date": "2026-03-01",
            "sensor_id": "sensor-1",
            "location_id": "site-1",
            "latitude": 40.0,
            "longitude": -70.0,
            **{
                key: value
                for key, value in hourly.items()
                if key not in ("hour_start", "hour_end")
            },
        }

        with patch(
            "src.storage.postgres_writer.get_postgres_connection",
            return_value=connection,
        ):
            self.assertTrue(
                persist_event_summary_batch(
                    "topic-test", "run-1", 5, [hourly], [daily]
                )
            )
            self.assertFalse(
                persist_event_summary_batch(
                    "topic-test", "run-1", 5, [hourly], [daily]
                )
            )

        writes = [
            statement
            for statement, _ in connection.executions
            if "INSERT INTO iot_global_hourly_summaries" in statement
            or "INSERT INTO iot_daily_sensor_summaries" in statement
        ]
        self.assertEqual(len(writes), 2)
        self.assertEqual(connection.close_count, 2)
        hourly_params = next(
            params
            for statement, params in connection.executions
            if "INSERT INTO iot_global_hourly_summaries" in statement
        )
        self.assertEqual(hourly_params[2].tzinfo, timezone.utc)
        self.assertEqual(hourly_params[3].tzinfo, timezone.utc)
        self.assertNotIn(
            "sensor_events",
            " ".join(writes),
        )
        daily_statement, daily_params = next(
            (statement, params)
            for statement, params in connection.executions
            if "INSERT INTO iot_daily_sensor_summaries" in statement
        )
        self.assert_insert_arity(daily_statement, daily_params)
        self.assertEqual(daily_params[4], _location_key(daily))
        self.assertIn(
            "ON CONFLICT (topic, run_id, summary_date, sensor_id, location_key)",
            " ".join(daily_statement.split()),
        )

    def test_window_status_and_anomalies_are_written_once_per_batch(self):
        connection = FakeConnection()
        status = {
            "hour_start": "2026-03-01T00:00:00Z",
            "normal_count": 5,
            "anomaly_suspected_count": 1,
            "insufficient_data_count": 2,
            "other_count": 0,
        }
        anomaly = {
            "sensor_id": "sensor-1",
            "location_id": "site-1",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-03-01T00:10:00Z",
            "latitude": 40.0,
            "longitude": -70.0,
            "measurement_count": 3,
            "pressure_observation_count": 3,
            "avg_pressure_pa": 100900.0,
            "min_pressure_pa": 100800.0,
            "max_pressure_pa": 102100.0,
            "pressure_range_pa": 1300.0,
            "pressure_variation_anomaly": True,
            "temperature_observation_count": 3,
            "avg_temperature_c": 10.0,
            "min_temperature_c": 8.0,
            "max_temperature_c": 15.0,
            "temperature_range_c": 7.0,
            "temperature_variation_anomaly": True,
            "humidity_observation_count": 3,
            "avg_humidity_pct": 52.0,
            "min_humidity_pct": 42.0,
            "max_humidity_pct": 70.0,
            "humidity_range_pct": 28.0,
            "humidity_variation_anomaly": True,
            "anomaly_status": "anomaly_suspected",
        }

        with patch(
            "src.storage.postgres_writer.get_postgres_connection",
            return_value=connection,
        ):
            self.assertTrue(
                persist_window_summary_batch(
                    "topic-test", "run-1", 8, [status], [anomaly]
                )
            )
            self.assertFalse(
                persist_window_summary_batch(
                    "topic-test", "run-1", 8, [status], [anomaly]
                )
            )

        writes = [
            statement
            for statement, _ in connection.executions
            if "INSERT INTO iot_window_status_hourly" in statement
            or "INSERT INTO iot_sensor_window_anomalies" in statement
        ]
        self.assertEqual(len(writes), 2)
        anomaly_statement, anomaly_params = next(
            (statement, params)
            for statement, params in connection.executions
            if "INSERT INTO iot_sensor_window_anomalies" in statement
        )
        self.assert_insert_arity(anomaly_statement, anomaly_params)
        self.assertEqual(anomaly_params[3], _location_key(anomaly))
        self.assertIn(
            "location_key",
            " ".join(anomaly_statement.split()),
        )


if __name__ == "__main__":
    unittest.main()
