"""Local Spark regressions for Kafka JSON parsing and anomaly windows."""

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, when

from src.streaming.kafka_spark_consumer import (
    _collect_window_status_summary_rows,
    _write_silo_batch,
    _write_silo_event_batch,
    build_event_summary_frames,
    build_windowed_metrics,
    parse_and_classify_kafka_messages,
    persist_window_metrics_batch,
    route_batch,
)
from src.streaming.kafka_consumer_config import (
    silo_event_batch_path,
    silo_window_run_batch_path,
)


TEST_KAFKA_TOPIC = "iot-sensor-events-anomaly-test-v2"


def make_event(
    timestamp="2026-03-02 00:00:00",
    pressure=100850.0,
    temperature=8.0,
    humidity=42.0,
    schema_version="1.1",
    sensor_id="anomaly-test-sensor-001",
):
    """Build one valid canonical event payload for parser tests."""
    return {
        "schema_version": schema_version,
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "location": {
            "location_id": "test-lab",
            "latitude": 49.9,
            "longitude": 8.6,
        },
        "measurements": {
            "pressure_raw": str(pressure),
            "pressure_numeric": pressure,
            "pressure_pa_candidate": pressure,
            "pressure_unit_candidate": "Pa",
            "temperature_c": temperature,
            "humidity_pct": humidity,
        },
        "quality": {
            "pressure_status": "valid_pa",
            "temperature_status": "valid_temperature",
            "humidity_status": "valid_humidity",
            "humidity_at_boundary": False,
            "record_status": "accepted_candidate",
        },
    }


def kafka_source_frame(spark, payloads):
    """Build Kafka-shaped rows with JVM SQL expressions, avoiding Python RDDs."""
    payload_column = lit(None).cast("string")
    for offset, payload in reversed(list(enumerate(payloads))):
        payload_column = when(
            col("id") == lit(offset),
            lit(payload.decode("utf-8")),
        ).otherwise(payload_column)

    return spark.range(len(payloads)).select(
        lit(TEST_KAFKA_TOPIC).alias("topic"),
        lit(0).cast("int").alias("partition"),
        col("id").cast("long").alias("offset"),
        to_timestamp(lit("2026-03-02 00:03:00")).alias("timestamp"),
        payload_column.cast("binary").alias("value"),
    )


class TestKafkaSparkConsumerJson(unittest.TestCase):
    """Exercise the production parser and window builder with local data."""

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("kafka-json-bom-regression")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def parse_payloads(self, payloads):
        source = kafka_source_frame(self.spark, payloads)
        return parse_and_classify_kafka_messages(source).orderBy("offset").collect()

    def test_plain_and_bom_prefixed_json_follow_the_same_accepted_path(self):
        payload = json.dumps(make_event()).encode("utf-8")
        rows = self.parse_payloads([payload, b"\xef\xbb\xbf" + payload])

        self.assertEqual(
            [row.processing_status for row in rows],
            ["accepted", "accepted"],
        )
        self.assertEqual([row.json_parsed for row in rows], [True, True])
        self.assertEqual(rows[0].sensor_id, rows[1].sensor_id)
        self.assertEqual(rows[0].schema_version, rows[1].schema_version)
        self.assertEqual(rows[0].event_timestamp, rows[1].event_timestamp)
        self.assertEqual(
            rows[0].pressure_pa_candidate,
            rows[1].pressure_pa_candidate,
        )

    def test_invalid_json_keeps_its_invalid_json_status(self):
        row = self.parse_payloads([b"{not-valid-json"])[0]

        self.assertFalse(row.json_parsed)
        self.assertEqual(row.processing_status, "invalid_json")

    def test_unsupported_schema_remains_distinct_from_invalid_json(self):
        payload = json.dumps(make_event(schema_version="9.9")).encode("utf-8")
        row = self.parse_payloads([payload])[0]

        self.assertTrue(row.json_parsed)
        self.assertEqual(row.schema_version, "9.9")
        self.assertEqual(row.processing_status, "unsupported_schema")

    def test_bom_inside_a_json_string_is_preserved(self):
        expected_sensor_id = "anomaly-\ufeffsensor-001"
        payload = json.dumps(
            make_event(sensor_id=expected_sensor_id)
        ).encode("utf-8")
        row = self.parse_payloads([payload])[0]

        self.assertEqual(row.processing_status, "accepted")
        self.assertEqual(row.sensor_id, expected_sensor_id)

    def test_accepted_rows_keep_event_lineage_and_measurements(self):
        payload = json.dumps(make_event()).encode("utf-8")
        row = self.parse_payloads([payload])[0]

        self.assertEqual(row.topic, TEST_KAFKA_TOPIC)
        self.assertEqual(row.partition, 0)
        self.assertEqual(row.offset, 0)
        self.assertEqual(row.sensor_id, "anomaly-test-sensor-001")
        self.assertIsNotNone(row.event_timestamp)
        self.assertEqual(row.pressure_raw, "100850.0")
        self.assertEqual(row.pressure_numeric, 100850.0)
        self.assertEqual(row.pressure_pa_candidate, 100850.0)
        self.assertEqual(row.temperature_c, 8.0)
        self.assertEqual(row.humidity_pct, 42.0)
        self.assertEqual(row.processing_status, "accepted")
        self.assertTrue(row.event_json.startswith("{"))

    def test_invalid_timestamp_remains_distinct_from_invalid_json(self):
        payload = json.dumps(
            make_event(timestamp="not-a-timestamp")
        ).encode("utf-8")
        row = self.parse_payloads([payload])[0]

        self.assertTrue(row.json_parsed)
        self.assertEqual(row.processing_status, "invalid_timestamp")

    def test_route_batch_logs_all_statuses_and_persists_only_accepted(self):
        invalid_payload = b"{not-valid-json"
        unsupported_payload = json.dumps(
            make_event(schema_version="9.9")
        ).encode("utf-8")
        source = kafka_source_frame(
            self.spark,
            [
                json.dumps(make_event()).encode("utf-8"),
                invalid_payload,
                unsupported_payload,
            ],
        )
        classified = parse_and_classify_kafka_messages(source)

        with patch(
            "src.streaming.kafka_spark_consumer.persist_sensor_events_partition"
        ) as persist_events, patch(
            "src.streaming.kafka_spark_consumer.EVENT_PERSISTENCE_MODE",
            "postgres",
        ):
            with self.assertLogs(
                "src.streaming.kafka_spark_consumer", level="INFO"
            ) as logs:
                route_batch(classified, batch_id=23)

        persisted_rows = persist_events.call_args.args[0]
        self.assertEqual(len(persisted_rows), 1)
        self.assertEqual(persisted_rows[0].processing_status, "accepted")
        self.assertTrue(
            any(
                "total=3 accepted=1 nonaccepted=2" in record.getMessage()
                for record in logs.records
            )
        )

    def test_silo_summary_mode_archives_events_and_persists_compact_summaries(self):
        source = kafka_source_frame(
            self.spark,
            [
                json.dumps(make_event()).encode("utf-8"),
                json.dumps(
                    make_event(
                        timestamp="2026-03-02 01:15:00",
                        sensor_id="anomaly-test-sensor-002",
                        pressure=100900.0,
                    )
                ).encode("utf-8"),
                b"{not-valid-json",
            ],
        )
        classified = parse_and_classify_kafka_messages(source)

        with patch(
            "src.streaming.kafka_spark_consumer.EVENT_PERSISTENCE_MODE",
            "silo",
        ), patch(
            "src.streaming.kafka_spark_consumer.WINDOW_PERSISTENCE_MODE",
            "silo_summaries",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            TEST_KAFKA_TOPIC,
        ), patch(
            "src.streaming.kafka_spark_consumer.EVENT_SILO_RUN_ID",
            "unit-test-run",
        ), patch(
            "src.streaming.kafka_spark_consumer._write_silo_event_batch"
        ) as write_events, patch(
            "src.streaming.kafka_spark_consumer.persist_sensor_events_partition"
        ) as persist_event_details, patch(
            "src.streaming.kafka_spark_consumer.persist_event_summary_batch",
            return_value=True,
        ) as persist_summaries:
            route_batch(classified, batch_id=31)

        write_events.assert_called_once_with(
            classified,
            silo_event_batch_path(TEST_KAFKA_TOPIC, "unit-test-run", 31),
        )
        persist_event_details.assert_not_called()
        summary_args = persist_summaries.call_args.args
        self.assertEqual(summary_args[:3], (TEST_KAFKA_TOPIC, "unit-test-run", 31))
        hourly_rows, daily_rows = summary_args[3:]
        self.assertEqual(len(hourly_rows), 2)
        self.assertEqual(len(daily_rows), 2)
        self.assertEqual(
            sum(row.accepted_event_count for row in hourly_rows),
            2,
        )
        self.assertEqual(
            sum(row.accepted_event_count for row in daily_rows),
            2,
        )

    def test_event_summaries_count_accepted_events_once_at_both_grains(self):
        source = kafka_source_frame(
            self.spark,
            [
                json.dumps(make_event()).encode("utf-8"),
                json.dumps(
                    make_event(
                        timestamp="2026-03-02 00:04:00",
                        pressure=100950.0,
                    )
                ).encode("utf-8"),
                b"{not-valid-json",
            ],
        )
        classified = parse_and_classify_kafka_messages(source)
        hourly, daily = build_event_summary_frames(classified)
        hourly_rows = hourly.collect()
        daily_rows = daily.collect()

        self.assertEqual(len(hourly_rows), 1)
        self.assertEqual(len(daily_rows), 1)
        self.assertEqual(hourly_rows[0].accepted_event_count, 2)
        self.assertEqual(daily_rows[0].accepted_event_count, 2)
        self.assertEqual(hourly_rows[0].pressure_observation_count, 2)
        self.assertEqual(hourly_rows[0].pressure_sum, 201800.0)
        self.assertEqual(daily_rows[0].sensor_id, "anomaly-test-sensor-001")

    def test_window_summary_mode_writes_run_scoped_detail_and_compact_pg(self):
        events = [
            make_event(
                timestamp="2026-03-02 00:00:00",
                pressure=100850.0,
                temperature=8.0,
                humidity=42.0,
            ),
            make_event(
                timestamp="2026-03-02 00:01:00",
                pressure=100900.0,
                temperature=9.0,
                humidity=45.0,
            ),
            make_event(
                timestamp="2026-03-02 00:02:00",
                pressure=102100.0,
                temperature=15.0,
                humidity=70.0,
            ),
        ]
        source = kafka_source_frame(
            self.spark,
            [json.dumps(event).encode("utf-8") for event in events],
        )
        classified = parse_and_classify_kafka_messages(source)
        metrics = build_windowed_metrics(classified)
        expected_window_rows = metrics.collect()
        expected_path = silo_window_run_batch_path(
            TEST_KAFKA_TOPIC,
            "unit-test-run",
            41,
        )

        with patch(
            "src.streaming.kafka_spark_consumer.WINDOW_PERSISTENCE_MODE",
            "silo_summaries",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            TEST_KAFKA_TOPIC,
        ), patch(
            "src.streaming.kafka_spark_consumer.EVENT_SILO_RUN_ID",
            "unit-test-run",
        ), patch(
            "src.streaming.kafka_spark_consumer._write_silo_batch"
        ) as write_silo, patch(
            "src.streaming.kafka_spark_consumer.persist_window_metrics_partition"
        ) as persist_all_windows, patch(
            "src.streaming.kafka_spark_consumer.persist_window_summary_batch",
            return_value=True,
        ) as persist_compact:
            persist_window_metrics_batch(metrics, batch_id=41)

        write_silo.assert_called_once_with(metrics, expected_path)
        persist_all_windows.assert_not_called()
        summary_args = persist_compact.call_args.args
        self.assertEqual(summary_args[:3], (TEST_KAFKA_TOPIC, "unit-test-run", 41))
        status_rows, anomaly_rows = summary_args[3:]
        self.assertTrue(status_rows)
        expected_counts = {}
        for row in expected_window_rows:
            hour_start = row.window_start.replace(
                minute=0,
                second=0,
                microsecond=0,
            )
            status = row.anomaly_status
            if status not in {
                "normal",
                "anomaly_suspected",
                "insufficient_data",
            }:
                status = "other"
            expected_counts.setdefault(hour_start, Counter())[status] += 1
        expected_status_rows = [
            {
                "hour_start": hour_start,
                "normal_count": counts.get("normal", 0),
                "anomaly_suspected_count": counts.get(
                    "anomaly_suspected", 0
                ),
                "insufficient_data_count": counts.get(
                    "insufficient_data", 0
                ),
                "other_count": counts.get("other", 0),
            }
            for hour_start, counts in sorted(expected_counts.items())
        ]
        self.assertEqual(status_rows, expected_status_rows)
        expected_anomalies = [
            row
            for row in expected_window_rows
            if row.anomaly_status == "anomaly_suspected"
        ]
        self.assertEqual(
            [
                (row.window_start, row.sensor_id, row.measurement_count)
                for row in anomaly_rows
            ],
            [
                (row.window_start, row.sensor_id, row.measurement_count)
                for row in expected_anomalies
            ],
        )

    def test_window_status_spark_aggregation_covers_known_and_unknown_states(
        self,
    ):
        states = self.spark.range(5).select(
            when(
                col("id") < 3,
                to_timestamp(lit("2026-03-01 10:15:00")),
            )
            .otherwise(to_timestamp(lit("2026-03-01 11:15:00")))
            .alias("window_start"),
            when(col("id") == 0, lit("normal"))
            .when(col("id") == 1, lit("anomaly_suspected"))
            .when(col("id") == 2, lit("insufficient_data"))
            .when(col("id") == 3, lit("future_status"))
            .otherwise(lit(None).cast("string"))
            .alias("anomaly_status"),
        )

        status_rows = _collect_window_status_summary_rows(states)

        self.assertEqual(
            status_rows,
            [
                {
                    "hour_start": datetime(2026, 3, 1, 10),
                    "normal_count": 1,
                    "anomaly_suspected_count": 1,
                    "insufficient_data_count": 1,
                    "other_count": 0,
                },
                {
                    "hour_start": datetime(2026, 3, 1, 11),
                    "normal_count": 0,
                    "anomaly_suspected_count": 0,
                    "insufficient_data_count": 0,
                    "other_count": 2,
                },
            ],
        )

    def test_silo_mode_archives_classified_rows_to_run_scoped_batches(self):
        source = kafka_source_frame(
            self.spark,
            [
                json.dumps(make_event()).encode("utf-8"),
                b"{not-valid-json",
            ],
        )
        classified = parse_and_classify_kafka_messages(source)
        expected_first_path = silo_event_batch_path(
            TEST_KAFKA_TOPIC,
            "unit-test-run",
            23,
        )
        expected_next_path = silo_event_batch_path(
            TEST_KAFKA_TOPIC,
            "unit-test-run",
            24,
        )

        with patch(
            "src.streaming.kafka_spark_consumer.EVENT_PERSISTENCE_MODE",
            "silo",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            TEST_KAFKA_TOPIC,
        ), patch(
            "src.streaming.kafka_spark_consumer.EVENT_SILO_RUN_ID",
            "unit-test-run",
        ), patch(
            "src.streaming.kafka_spark_consumer.persist_sensor_events_partition"
        ) as persist_events, patch(
            "src.streaming.kafka_spark_consumer._write_silo_event_batch"
        ) as write_silo:
            route_batch(classified, batch_id=23)
            route_batch(classified, batch_id=23)
            route_batch(classified, batch_id=24)

        persist_events.assert_not_called()
        self.assertEqual(write_silo.call_count, 3)
        paths = [call.args[1] for call in write_silo.call_args_list]
        self.assertEqual(
            paths,
            [expected_first_path, expected_first_path, expected_next_path],
        )
        for call in write_silo.call_args_list:
            self.assertEqual(
                sorted(
                    row.processing_status
                    for row in call.args[0].collect()
                ),
                ["accepted", "invalid_json"],
            )

    def test_silo_batch_writer_overwrites_only_the_same_retry_path(self):
        source = kafka_source_frame(
            self.spark,
            [
                json.dumps(make_event()).encode("utf-8"),
                b"{not-valid-json",
            ],
        )
        classified = parse_and_classify_kafka_messages(source)

        with tempfile.TemporaryDirectory(prefix="accepted-event-batch-") as root:
            batch_path = str(Path(root) / "batch_id=00000000000000000023")
            _write_silo_event_batch(classified, batch_path)
            _write_silo_event_batch(classified, batch_path)

            rows = self.spark.read.parquet(batch_path).collect()

        self.assertEqual(len(rows), 2)
        by_status = {row.processing_status: row for row in rows}
        self.assertEqual(set(by_status), {"accepted", "invalid_json"})
        accepted = by_status["accepted"]
        self.assertEqual(accepted.topic, TEST_KAFKA_TOPIC)
        self.assertEqual(accepted.partition, 0)
        self.assertEqual(accepted.offset, 0)
        self.assertEqual(accepted.sensor_id, "anomaly-test-sensor-001")
        self.assertEqual(accepted.pressure_pa_candidate, 100850.0)
        self.assertEqual(accepted.processing_status, "accepted")
        self.assertEqual(accepted.event_json, json.dumps(make_event()))
        self.assertEqual(by_status["invalid_json"].offset, 1)
        self.assertEqual(
            by_status["invalid_json"].event_json,
            "{not-valid-json",
        )

    def test_window_batch_writer_round_trips_detail_to_local_parquet(self):
        batch = self.spark.range(2).select(
            col("id").cast("long").alias("offset"),
            lit("sensor-1").alias("sensor_id"),
            to_timestamp(lit("2026-03-01 10:00:00")).alias("window_start"),
            lit(3).cast("long").alias("measurement_count"),
            lit("anomaly_suspected").alias("anomaly_status"),
        )

        with tempfile.TemporaryDirectory(prefix="window-detail-batch-") as root:
            output_path = str(Path(root) / "batch_id=00000000000000000031")
            _write_silo_batch(batch, output_path)
            _write_silo_batch(batch, output_path)
            persisted_rows = (
                self.spark.read.parquet(output_path)
                .orderBy("offset")
                .collect()
            )

        self.assertEqual(len(persisted_rows), 2)
        self.assertEqual([row.offset for row in persisted_rows], [0, 1])
        self.assertTrue(
            all(row.sensor_id == "sensor-1" for row in persisted_rows)
        )
        self.assertTrue(
            all(row.measurement_count == 3 for row in persisted_rows)
        )
        self.assertTrue(
            all(
                row.anomaly_status == "anomaly_suspected"
                for row in persisted_rows
            )
        )

    def test_three_anomaly_events_produce_expected_window_ranges(self):
        events = [
            make_event(
                timestamp="2026-03-02 00:00:00",
                pressure=100850.0,
                temperature=8.0,
                humidity=42.0,
            ),
            make_event(
                timestamp="2026-03-02 00:01:00",
                pressure=100900.0,
                temperature=9.0,
                humidity=45.0,
            ),
            make_event(
                timestamp="2026-03-02 00:02:00",
                pressure=102100.0,
                temperature=15.0,
                humidity=70.0,
            ),
        ]
        payloads = [json.dumps(event).encode("utf-8") for event in events]
        source = kafka_source_frame(self.spark, payloads)
        classified = parse_and_classify_kafka_messages(source)

        self.assertEqual(
            [
                row.processing_status
                for row in classified.orderBy("offset").collect()
            ],
            ["accepted", "accepted", "accepted"],
        )

        metrics = build_windowed_metrics(classified).collect()
        complete_windows = [row for row in metrics if row.measurement_count == 3]

        self.assertTrue(complete_windows)
        for row in complete_windows:
            self.assertEqual(row.pressure_observation_count, 3)
            self.assertEqual(row.temperature_observation_count, 3)
            self.assertEqual(row.humidity_observation_count, 3)
            self.assertEqual(row.pressure_range_pa, 1250.0)
            self.assertEqual(row.temperature_range_c, 7.0)
            self.assertEqual(row.humidity_range_pct, 28.0)
            self.assertEqual(row.anomaly_status, "anomaly_suspected")


if __name__ == "__main__":
    unittest.main()
