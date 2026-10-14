"""Pure Python tests for compact window-status projection."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.streaming.kafka_spark_consumer import (
    persist_window_metrics_batch,
)
from src.streaming.kafka_consumer_config import (
    silo_window_batch_path,
    silo_window_run_batch_path,
)


class TestWindowSummaryHelpers(unittest.TestCase):
    def test_window_batch_retry_reuses_only_its_run_scoped_silo_path(self):
        status_rows = [
            {
                "hour_start": "2026-03-01 10:00:00",
                "normal_count": 1,
                "anomaly_suspected_count": 1,
                "insufficient_data_count": 0,
                "other_count": 0,
            }
        ]
        anomaly = SimpleNamespace(anomaly_status="anomaly_suspected")
        batch_df = MagicMock()
        anomaly_frame = MagicMock()
        anomaly_frame.collect.return_value = [anomaly]
        batch_df.filter.return_value = anomaly_frame
        topic = "iot-sensor-events-march-summary-test-v1"
        run_id = "summary-unit-run"
        retry_path = silo_window_run_batch_path(topic, run_id, 12)
        next_path = silo_window_run_batch_path(topic, run_id, 13)

        with patch(
            "src.streaming.kafka_spark_consumer.WINDOW_PERSISTENCE_MODE",
            "silo_summaries",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            topic,
        ), patch(
            "src.streaming.kafka_spark_consumer.EVENT_SILO_RUN_ID",
            run_id,
        ), patch(
            "src.streaming.kafka_spark_consumer.col",
        ), patch(
            "src.streaming.kafka_spark_consumer._collect_window_status_summary_rows",
            return_value=status_rows,
        ) as collect_status, patch(
            "src.streaming.kafka_spark_consumer._write_silo_batch"
        ) as write_silo, patch(
            "src.streaming.kafka_spark_consumer.persist_window_metrics_partition"
        ) as persist_detailed, patch(
            "src.streaming.kafka_spark_consumer.persist_window_summary_batch",
            return_value=True,
        ) as persist_summary:
            persist_window_metrics_batch(batch_df, 12)
            persist_window_metrics_batch(batch_df, 12)
            persist_window_metrics_batch(batch_df, 13)

        self.assertEqual(
            [call.args[1] for call in write_silo.call_args_list],
            [retry_path, retry_path, next_path],
        )
        self.assertEqual(persist_summary.call_count, 3)
        self.assertEqual(collect_status.call_count, 3)
        self.assertEqual(batch_df.persist.call_count, 3)
        self.assertEqual(batch_df.unpersist.call_count, 3)
        batch_df.collect.assert_not_called()
        self.assertEqual(anomaly_frame.collect.call_count, 3)
        for call in persist_summary.call_args_list:
            self.assertEqual(call.args[3], status_rows)
            self.assertEqual(call.args[4], [anomaly])
        persist_detailed.assert_not_called()

    def test_empty_strategy_batch_claims_progress_without_writing_silo(self):
        batch_df = MagicMock()

        with patch(
            "src.streaming.kafka_spark_consumer.WINDOW_PERSISTENCE_MODE",
            "silo_summaries",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            "iot-sensor-events-march-pipeline",
        ), patch(
            "src.streaming.kafka_spark_consumer.EVENT_SILO_RUN_ID",
            None,
        ), patch(
            "src.streaming.kafka_spark_consumer._collect_window_status_summary_rows",
            return_value=[],
        ), patch(
            "src.streaming.kafka_spark_consumer._write_silo_batch"
        ) as write_silo, patch(
            "src.streaming.kafka_spark_consumer.persist_window_summary_batch",
            return_value=True,
        ) as persist_summary:
            persist_window_metrics_batch(batch_df, 17)

        persist_summary.assert_called_once_with(
            "iot-sensor-events-march-pipeline",
            None,
            17,
            [],
            [],
        )
        write_silo.assert_not_called()
        batch_df.filter.assert_not_called()
        batch_df.collect.assert_not_called()
        batch_df.unpersist.assert_called_once_with()

    def test_historical_postgres_mode_keeps_full_row_persistence(self):
        rows = [SimpleNamespace(anomaly_status="normal")]
        batch_df = MagicMock()
        batch_df.collect.return_value = rows
        topic = "iot-sensor-events-historical-window-test-v1"

        with patch(
            "src.streaming.kafka_spark_consumer.WINDOW_PERSISTENCE_MODE",
            "postgres",
        ), patch(
            "src.streaming.kafka_spark_consumer.KAFKA_TOPIC",
            topic,
        ), patch(
            "src.streaming.kafka_spark_consumer.persist_window_metrics_partition"
        ) as persist_detailed, patch(
            "src.streaming.kafka_spark_consumer._write_silo_batch"
        ) as write_silo, patch(
            "src.streaming.kafka_spark_consumer.persist_window_summary_batch"
        ) as persist_summary:
            persist_window_metrics_batch(batch_df, 21)

        batch_df.collect.assert_called_once_with()
        persist_detailed.assert_called_once_with(rows)
        write_silo.assert_called_once_with(
            batch_df,
            silo_window_batch_path(topic, 21),
        )
        persist_summary.assert_not_called()
        batch_df.persist.assert_not_called()
        batch_df.unpersist.assert_not_called()


if __name__ == "__main__":
    unittest.main()
