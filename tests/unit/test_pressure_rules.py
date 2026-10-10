
import unittest

from src.streaming.pressure_rules import normalize_pressure


class TestNormalizePressure(unittest.TestCase):
    """Test pressure normalization and data quality classification."""

    def test_pressure_values(self):
        cases = [
            (1000, 100000.0, "hPa_candidate", "converted_from_hpa"),
            (101325, 101325.0, "Pa", "valid_pa"),
            (
                1127.59,
                112759.0,
                "hPa_candidate",
                "converted_from_hpa_out_of_range",
            ),
            (-19420.81, None, "unknown", "invalid_nonpositive"),
            (0, None, "unknown", "invalid_nonpositive"),
            (None, None, "unknown", "missing_or_nonnumeric"),
            ("invalid", None, "unknown", "missing_or_nonnumeric"),
            (14643.25, None, "unknown", "outside_candidate_ranges"),
        ]

        for value, expected_pressure, expected_unit, expected_status in cases:
            with self.subTest(value=value):
                result = normalize_pressure(value)

                self.assertEqual(result[1], expected_unit)
                self.assertEqual(result[2], expected_status)

                if expected_pressure is None:
                    self.assertIsNone(result[0])
                else:
                    self.assertAlmostEqual(
                        result[0],
                        expected_pressure,
                    )


if __name__ == "__main__":
    unittest.main()
