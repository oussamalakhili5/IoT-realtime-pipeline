
"""Build and validate canonical IoT events."""

from collections.abc import Mapping
from datetime import date, datetime
import math
from numbers import Real
from typing import Any


SCHEMA_VERSION = "1.1"


def _is_missing(value: Any) -> bool:
    """Return whether a scalar value represents missing data."""
    if value is None:
        return True

    try:
        return math.isnan(value)
    except (TypeError, ValueError, OverflowError):
        return False


def _required_text(row: Mapping[str, Any], field: str) -> str:
    """Return a required field as non-empty text."""
    value = row.get(field)

    if _is_missing(value):
        raise ValueError(f"Required field '{field}' is missing.")

    if field == "sensor_id" and isinstance(value, Real):
        if not isinstance(value, bool):
            numeric_value = float(value)
            if math.isfinite(numeric_value) and numeric_value.is_integer():
                return str(int(numeric_value))

    if isinstance(value, (datetime, date)):
        text_value = value.isoformat()
    else:
        text_value = str(value).strip()

    if not text_value:
        raise ValueError(f"Required field '{field}' is empty.")

    return text_value


def _optional_number(value: Any) -> float | None:
    """Convert a value to a finite float or None."""
    if _is_missing(value):
        return None

    try:
        numeric_value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None

    if not math.isfinite(numeric_value):
        return None

    return numeric_value


def _optional_text(value: Any) -> str | None:
    """Convert an optional scalar value to non-empty text or None."""
    if _is_missing(value):
        return None

    text_value = str(value).strip()
    return text_value or None

def _required_status(row: Mapping[str, Any], field: str) -> str:
    """Return a non-empty quality status."""
    value = _optional_text(row.get(field))

    if not value:
        raise ValueError(
            f"Required quality status '{field}' is missing."
        )

    return value


def _first_available_number(
    row: Mapping[str, Any],
    *fields: str,
) -> float | None:
    """Return the first available numeric value among candidate fields."""
    for field in fields:
        if field in row and not _is_missing(row[field]):
            return _optional_number(row[field])

    return None


def _to_bool(value: Any) -> bool:
    """Convert a scalar value to a boolean safely."""
    if _is_missing(value):
        return False

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "1", "yes"}:
            return True

        if normalized in {"false", "0", "no"}:
            return False

        raise ValueError(
            f"Cannot interpret {value!r} as a boolean."
        )

    return bool(value)



def validate_event(event: Mapping[str, Any]) -> None:
    """Validate the structure and value types of a canonical event."""

    # Validate the top-level event structure.
    required_top_level = {
        "schema_version",
        "sensor_id",
        "timestamp",
        "location",
        "measurements",
        "quality",
    }

    missing_fields = required_top_level - set(event)

    if missing_fields:
        raise ValueError(
            f"Missing event fields: {sorted(missing_fields)}"
        )

    # Validate required textual fields.
    for field in ("schema_version", "sensor_id", "timestamp"):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ValueError(
                f"Event field '{field}' must be non-empty text."
            )

        # Extract nested objects and validate their types.
        location = event["location"]
        measurements = event["measurements"]
        quality = event["quality"]

        if not isinstance(location, Mapping):
            raise ValueError("'location' must be an object.")

        if not isinstance(measurements, Mapping):
            raise ValueError("'measurements' must be an object.")

        if not isinstance(quality, Mapping):
            raise ValueError("'quality' must be an object.")

        # Validate required location fields.
        required_location = {
            "location_id",
            "latitude",
            "longitude",
        }

        missing_location = required_location - set(location)

        if missing_location:
            raise ValueError(
                f"Missing location fields: {sorted(missing_location)}"
            )

        # Validate the optional location identifier.
        location_id = location["location_id"]

        if location_id is not None:
            if not isinstance(location_id, str) or not location_id.strip():
                raise ValueError(
                    "'location_id' must be non-empty text or None."
                )

            # Validate geographic coordinate types and ranges.
            coordinate_ranges = {
                "latitude": (-90.0, 90.0),
                "longitude": (-180.0, 180.0),
            }

            for field, (minimum, maximum) in coordinate_ranges.items():
                value = location[field]

                if value is None:
                    continue

                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    raise ValueError(
                        f"Location field '{field}' must be a finite "
                        "number or None."
                    )

                if not minimum <= value <= maximum:
                    raise ValueError(
                        f"Location field '{field}' must be between "
                        f"{minimum} and {maximum}."
                    )

                # Validate required measurement fields.
                required_measurements = {
                    "pressure_raw",
                    "pressure_numeric",
                    "pressure_pa_candidate",
                    "pressure_unit_candidate",
                    "temperature_c",
                    "humidity_pct",
                }

                missing_measurements = required_measurements - set(measurements)

                if missing_measurements:
                    raise ValueError(
                        f"Missing measurement fields: "
                        f"{sorted(missing_measurements)}"
                    )

                # Validate required quality fields.
                required_quality = {
                    "pressure_status",
                    "temperature_status",
                    "humidity_status",
                    "humidity_at_boundary",
                    "record_status",
                }

                missing_quality = required_quality - set(quality)

                if missing_quality:
                    raise ValueError(
                        f"Missing quality fields: {sorted(missing_quality)}"
                    )

                # Validate optional textual measurement fields.
                for field in ("pressure_raw", "pressure_unit_candidate"):
                    value = measurements[field]

                    if value is not None and not isinstance(value, str):
                        raise ValueError(
                            f"Measurement field '{field}' must be text or None."
                        )

                    # Validate numeric measurement fields.
                    numeric_fields = (
                        "pressure_numeric",
                        "pressure_pa_candidate",
                        "temperature_c",
                        "humidity_pct",
                    )

                    for field in numeric_fields:
                        value = measurements[field]

                        if value is not None:
                            if (
                                isinstance(value, bool)
                                or not isinstance(value, (int, float))
                                or not math.isfinite(value)
                            ):
                                raise ValueError(
                                    f"Measurement field '{field}' must be a finite "
                                    "number or None."
                                )

                            # Validate quality status fields.
                            status_fields = (
                                "pressure_status",
                                "temperature_status",
                                "humidity_status",
                                "record_status",
                            )

                            for field in status_fields:
                                value = quality[field]

                                if not isinstance(value, str) or not value.strip():
                                    raise ValueError(
                                        f"Quality field '{field}' must be non-empty text."
                                    )

                                # Validate the humidity boundary flag.
                                if not isinstance(quality["humidity_at_boundary"], bool):
                                    raise ValueError(
                                        "'humidity_at_boundary' must be a boolean."
                                    )


def build_event(row: Mapping[str, Any]) -> dict[str, Any]:
    """Convert an annotated sensor record into a canonical IoT event."""
    if not isinstance(row, Mapping):
        raise TypeError("The input row must be a mapping.")

    temperature = _first_available_number(
        row, "temperature_value", "temperature"
    )
    humidity = _first_available_number(
        row, "humidity_value", "humidity"
    )

    event = {
        "schema_version": SCHEMA_VERSION,
        "sensor_id": _required_text(row, "sensor_id"),
        "timestamp": _required_text(row, "timestamp"),
        "location": {
            "location_id": _optional_text(row.get("location")),
            "latitude": _optional_number(row.get("lat")),
            "longitude": _optional_number(row.get("lon")),
        },
        "measurements": {
            "pressure_raw": _optional_text(row.get("pressure_raw")),
            "pressure_numeric": _optional_number(
                row.get("pressure_numeric")
            ),
            "pressure_pa_candidate": _optional_number(
                row.get("pressure_pa_candidate")
            ),
            "pressure_unit_candidate": _optional_text(
                row.get("pressure_unit_candidate")
            ),
            "temperature_c": temperature,
            "humidity_pct": humidity,
        },
        "quality": {
            "pressure_status": _required_status(
                row, "pressure_status"
            ),
            "temperature_status": _required_status(
                row, "temperature_status"
            ),
            "humidity_status": _required_status(
                row, "humidity_status"
            ),
            "humidity_at_boundary": _to_bool(
                row.get("humidity_at_boundary")
            ),
            "record_status": _required_status(
                row, "record_status"
            ),
        },

    }

    validate_event(event)
    return event
