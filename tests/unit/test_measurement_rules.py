
import unittest

from src.streaming.measurement_rules import assess_measurement


class TestMeasurementRules(unittest.TestCase):
    """Test combined measurement quality assessment."""

    def test_valid_hpa_candidate(self):
        result = assess_measurement(1000, 20, 50)

        self.assertEqual(result["pressure_pa_candidate"], 100000.0)
        self.assertEqual(result["pressure_status"], "converted_from_hpa")
        self.assertEqual(result["record_status"], "accepted_candidate")

    def test_valid_pa_candidate(self):
        result = assess_measurement(101325, 20, 50)

        self.assertEqual(result["pressure_pa_candidate"], 101325.0)
        self.assertEqual(result["pressure_status"], "valid_pa")
        self.assertEqual(result["record_status"], "accepted_candidate")

    def test_temperature_out_of_range(self):
        result = assess_measurement(1000, -147.57, 100)

        self.assertEqual(
            result["temperature_status"],
            "temperature_out_of_range",
        )
        self.assertTrue(result["humidity_at_boundary"])
        self.assertEqual(result["record_status"], "review_required")

    def test_humidity_out_of_range(self):
        result = assess_measurement(101325, 20, 157)

        self.assertEqual(
            result["humidity_status"],
            "humidity_out_of_range",
        )
        self.assertEqual(result["record_status"], "review_required")

    def test_pressure_out_of_range(self):
        result = assess_measurement(14643.25, 20, 50)

        self.assertEqual(
            result["pressure_status"],
            "outside_candidate_ranges",
        )
        self.assertEqual(result["record_status"], "review_required")


if __name__ == "__main__":
    unittest.main()
