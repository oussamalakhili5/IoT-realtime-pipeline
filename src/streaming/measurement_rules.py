
import math

from src.streaming.pressure_rules import normalize_pressure


TEMPERATURE_MIN = -40
TEMPERATURE_MAX = 85

HUMIDITY_MIN = 0
HUMIDITY_MAX = 100


def validate_numeric_range(value, minimum, maximum, field_name):
    """Validate whether a measurement is numeric and within its expected range."""
    if value is None:
        return None, f"missing_{field_name}"

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None, f"missing_{field_name}"

    if not math.isfinite(numeric_value):
        return None, f"missing_{field_name}"

    if minimum <= numeric_value <= maximum:
        return numeric_value, f"valid_{field_name}"

    return numeric_value, f"{field_name}_out_of_range"


def assess_measurement(pressure, temperature, humidity):
    """Evaluate pressure, temperature, and humidity independently."""
    pressure_pa, pressure_unit, pressure_status = normalize_pressure(
        pressure
    )

    temperature_value, temperature_status = validate_numeric_range(
        temperature,
        TEMPERATURE_MIN,
        TEMPERATURE_MAX,
        "temperature",
    )

    humidity_value, humidity_status = validate_numeric_range(
        humidity,
        HUMIDITY_MIN,
        HUMIDITY_MAX,
        "humidity",
    )

    accepted_pressure_statuses = {
        "valid_pa",
        "converted_from_hpa",
    }

    review_required = (
        pressure_status not in accepted_pressure_statuses
        or temperature_status != "valid_temperature"
        or humidity_status != "valid_humidity"
    )

    humidity_at_boundary = (
        humidity_value is not None
        and humidity_value in {HUMIDITY_MIN, HUMIDITY_MAX}
    )

    if review_required:
        record_status = "review_required"
    elif humidity_at_boundary:
        record_status = "accepted_with_humidity_boundary"
    else:
        record_status = "accepted_candidate"

    return {
        "pressure_pa_candidate": pressure_pa,
        "pressure_unit_candidate": pressure_unit,
        "pressure_status": pressure_status,
        "temperature_value": temperature_value,
        "temperature_status": temperature_status,
        "humidity_value": humidity_value,
        "humidity_status": humidity_status,
        "humidity_at_boundary": humidity_at_boundary,
        "record_status": record_status,
    }
