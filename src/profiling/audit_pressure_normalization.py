
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")

SUMMARY_PATH = Path("reports/pressure_normalization_audit_summary.csv")
SENSOR_PATH = Path("reports/pressure_normalization_audit_by_sensor.csv")

CHUNK_SIZE = 500_000

STATUSES = [
    "missing_or_nonnumeric",
    "invalid_nonpositive",
    "converted_from_hpa",
    "converted_from_hpa_out_of_range",
    "valid_pa",
    "outside_candidate_ranges",
]


def classify_pressure_series(pressure: pd.Series) -> np.ndarray:
    """Classify pressure values using the current normalization rules."""
    values = pd.to_numeric(
        pressure, errors="coerce"
    ).to_numpy(dtype=float, na_value=np.nan)

    finite = np.isfinite(values)

    categories = np.full(
        len(values),
        "outside_candidate_ranges",
        dtype=object,
    )

    categories[~finite] = "missing_or_nonnumeric"
    categories[finite & (values <= 0)] = "invalid_nonpositive"

    hpa_candidate = (
        finite
        & (values >= 300)
        & (values <= 1200)
    )

    converted_values = values * 100

    hpa_valid = (
        hpa_candidate
        & (converted_values >= 30_000)
        & (converted_values <= 110_000)
    )

    categories[hpa_valid] = "converted_from_hpa"

    categories[
        hpa_candidate & ~hpa_valid
    ] = "converted_from_hpa_out_of_range"

    pa_candidate = (
        finite
        & (values >= 30_000)
        & (values <= 110_000)
    )

    categories[pa_candidate] = "valid_pa"

    return categories


def main() -> None:
    """Audit pressure normalization outcomes across the full dataset."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    status_counts = Counter()
    sensor_status_counts = Counter()
    total_rows = 0

    reader = pd.read_csv(
        DATA_PATH,
        sep=";",
        usecols=["sensor_id", "pressure"],
        dtype={"sensor_id": "string"},
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk["quality_status"] = classify_pressure_series(
            chunk["pressure"]
        )

        total_rows += len(chunk)

        for status, count in (
            chunk["quality_status"].value_counts().items()
        ):
            status_counts[str(status)] += int(count)

        grouped = chunk.groupby(
            ["sensor_id", "quality_status"],
            dropna=False,
        ).size()

        for (sensor_id, status), count in grouped.items():
            sensor_status_counts[
                (str(sensor_id), str(status))
            ] += int(count)

        if chunk_number % 25 == 0:
            print(
                f"Processed {chunk_number} chunks "
                f"({total_rows:,} rows)..."
            )

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    summary_records = []

    for status in STATUSES:
        count = status_counts[status]

        summary_records.append(
            {
                "quality_status": status,
                "records": count,
                "percentage": (
                    100 * count / total_rows if total_rows else 0
                ),
            }
        )

    pd.DataFrame(summary_records).to_csv(
        SUMMARY_PATH, index=False
    )

    sensor_records = [
        {
            "sensor_id": sensor_id,
            "quality_status": status,
            "records": count,
        }
        for (sensor_id, status), count in sensor_status_counts.items()
    ]

    pd.DataFrame(sensor_records).sort_values(
        ["sensor_id", "quality_status"]
    ).to_csv(SENSOR_PATH, index=False)

    print("\nPressure normalization audit completed.")
    print(f"Total rows: {total_rows:,}")
    print(f"Summary report: {SUMMARY_PATH}")
    print(f"Sensor report: {SENSOR_PATH}")
    print("\nResults by quality status:")

    print(pd.DataFrame(summary_records).to_string(index=False))


if __name__ == "__main__":
    main()
