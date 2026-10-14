"""Small JSON payload normalization helpers shared by ingestion paths."""

from typing import Any


UTF8_BOM = "\ufeff"
LEADING_UTF8_BOM_PATTERN = f"^{UTF8_BOM}"


def normalize_json_payload(payload: str | None) -> str | None:
    """Remove a UTF-8 BOM only if it is the first decoded character."""
    if payload is None or not payload.startswith(UTF8_BOM):
        return payload
    return payload[len(UTF8_BOM):]


def normalize_json_payload_column(payload_column: Any):
    """Return a Spark expression that strips only a leading UTF-8 BOM."""
    from pyspark.sql.functions import regexp_replace

    return regexp_replace(payload_column, LEADING_UTF8_BOM_PATTERN, "")
