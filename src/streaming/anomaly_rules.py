"""Rule-based classification of sensor-window anomalies."""

import math
from numbers import Real


MIN_OBSERVATIONS_PER_METRIC = 3

MAX_PRESSURE_RANGE_PA = 1000.0
MAX_TEMPERATURE_RANGE_C = 5.0
MAX_HUMIDITY_RANGE_PCT = 20.0


def classify_anomaly_status(
    pressure_observation_count: int,
    temperature_observation_count: int,
    humidity_observation_count: int,
    pressure_range_pa: float | None,
    temperature_range_c: float | None,
    humidity_range_pct: float | None,
) -> str:
    """Classify a window using observation counts and value ranges."""

    observation_counts = (
        pressure_observation_count,
        temperature_observation_count,
        humidity_observation_count,
    )

    for count_value in observation_counts:
        if (
            isinstance(count_value, bool)
            or not isinstance(count_value, int)
            or count_value < 0
        ):
            return "insufficient_data"

        if count_value < MIN_OBSERVATIONS_PER_METRIC:
            return "insufficient_data"

    ranges_and_thresholds = (
        (pressure_range_pa, MAX_PRESSURE_RANGE_PA),
        (temperature_range_c, MAX_TEMPERATURE_RANGE_C),
        (humidity_range_pct, MAX_HUMIDITY_RANGE_PCT),
    )

    for observed_range, _ in ranges_and_thresholds:
        if (
            isinstance(observed_range, bool)
            or not isinstance(observed_range, Real)
            or not math.isfinite(observed_range)
            or observed_range < 0
        ):
            return "insufficient_data"

    if any(
        observed_range > threshold
        for observed_range, threshold in ranges_and_thresholds
    ):
        return "anomaly_suspected"

    return "normal"