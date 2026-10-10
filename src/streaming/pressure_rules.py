
import math


def normalize_pressure(value):
    """Return a candidate pressure in Pa, inferred unit, and quality status."""
    if value is None:
        return None, "unknown", "missing_or_nonnumeric"

    try:
        pressure = float(value)
    except (TypeError, ValueError):
        return None, "unknown", "missing_or_nonnumeric"

    if not math.isfinite(pressure):
        return None, "unknown", "missing_or_nonnumeric"

    if pressure <= 0:
        return None, "unknown", "invalid_nonpositive"

    # Values in this range are candidates for hPa readings.
    if 300 <= pressure <= 1200:
        pressure_pa = pressure * 100

        if 30_000 <= pressure_pa <= 110_000:
            return pressure_pa, "hPa_candidate", "converted_from_hpa"

        return (
            pressure_pa,
            "hPa_candidate",
            "converted_from_hpa_out_of_range",
        )

    # Values in this range are candidates for readings already expressed in Pa.
    if 30_000 <= pressure <= 110_000:
        return pressure, "Pa", "valid_pa"

    return None, "unknown", "outside_candidate_ranges"
