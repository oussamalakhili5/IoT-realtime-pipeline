"""Unit tests for window-level anomaly classification."""

import unittest

from src.streaming.anomaly_rules import classify_anomaly_status


class TestAnomalyRules(unittest.TestCase):
    """Test observation-count and variation thresholds."""

    def classify(
        self,
        pressure_count=3,
        temperature_count=3,
        humidity_count=3,
        pressure_range=0.0,
        temperature_range=1.0,
        humidity_range=0.0,
    ):
        """Classify a window using default values and overrides."""
        return classify_anomaly_status(
            pressure_count,
            temperature_count,
            humidity_count,
            pressure_range,
            temperature_range,
            humidity_range,
        )

    def test_insufficient_observations(self):
        self.assertEqual(
            self.classify(temperature_count=2),
            "insufficient_data",
        )

    def test_stable_window_is_normal(self):
        self.assertEqual(
            self.classify(),
            "normal",
        )

    def test_temperature_variation_is_suspected_anomaly(self):
        self.assertEqual(
            self.classify(temperature_range=23.0),
            "anomaly_suspected",
        )

    def test_pressure_and_humidity_variations(self):
        with self.subTest(measurement="pressure"):
            self.assertEqual(
                self.classify(pressure_range=1001.0),
                "anomaly_suspected",
            )

        with self.subTest(measurement="humidity"):
            self.assertEqual(
                self.classify(humidity_range=21.0),
                "anomaly_suspected",
            )

    def test_exact_threshold_is_not_an_anomaly(self):
        self.assertEqual(
            self.classify(
                pressure_range=1000.0,
                temperature_range=5.0,
                humidity_range=20.0,
            ),
            "normal",
        )


if __name__ == "__main__":
    unittest.main()