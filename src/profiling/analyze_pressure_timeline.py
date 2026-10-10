
from pathlib import Path

import numpy as np
import pandas as pd


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")
OUTPUT_PATH = Path("reports/pressure_timeline_by_sensor_day.csv")

CHUNK_SIZE = 100_000

TARGET_SENSOR_IDS = {"39287", "93517", "54381", "78677"}

CATEGORIES = [
    "missing_or_nonnumeric",
    "zero_or_negative",
    "hPa_like",
    "Pa_like",
    "outside_candidate_ranges",
]



def classify_pressure(pressure: pd.Series) -> np.ndarray:
    """Classify pressure values using preliminary candidate ranges."""
    conditions = [
        pressure.isna(),
        pressure <= 0,
        pressure.between(300, 1200),
        pressure.between(30_000, 110_000),
    ]

    categories = [
        "missing_or_nonnumeric",
        "zero_or_negative",
        "hPa_like",
        "Pa_like",
    ]

    return np.select(
        conditions,
        categories,
        default="outside_candidate_ranges",
    )



def main() -> None:
    """Summarize pressure quality by sensor and calendar day."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    partial_results = []

    reader = pd.read_csv(
        DATA_PATH,
        sep=";",
        usecols=["sensor_id", "timestamp", "pressure"],
        dtype={"sensor_id": "string"},
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk[
            chunk["sensor_id"].isin(TARGET_SENSOR_IDS)
        ].copy()

        if chunk.empty:
            continue

        timestamp = (
            chunk["timestamp"]
            .astype("string")
            .str.strip()
            .str.strip("'\"")
        )

        parsed_timestamp = pd.to_datetime(
            timestamp,
            format="ISO8601",
            errors="coerce",
        )

        chunk["date"] = (
            parsed_timestamp.dt.strftime("%Y-%m-%d")
            .fillna("INVALID_TIMESTAMP")
        )

        chunk["pressure_numeric"] = pd.to_numeric(
            chunk["pressure"],
            errors="coerce",
        )

        chunk["pressure_category"] = classify_pressure(
            chunk["pressure_numeric"]
        )

        for category in CATEGORIES:
            chunk[f"count_{category}"] = (
                chunk["pressure_category"] == category
            ).astype("int32")

        aggregations = {
            "records": ("pressure_numeric", "size"),
            "numeric_pressure": ("pressure_numeric", "count"),
            "pressure_sum": ("pressure_numeric", "sum"),
            "pressure_min": ("pressure_numeric", "min"),
            "pressure_max": ("pressure_numeric", "max"),
        }

        for category in CATEGORIES:
            aggregations[f"count_{category}"] = (
                f"count_{category}",
                "sum",
            )

        daily = (
            chunk.groupby(["sensor_id", "date"])
            .agg(**aggregations)
            .reset_index()
        )

        partial_results.append(daily)

        if chunk_number % 50 == 0:
            print(f"Processed {chunk_number} chunks...")

    if not partial_results:
        raise RuntimeError("No records found for the target sensors.")

    combined = pd.concat(partial_results, ignore_index=True)

    aggregations = {
        "records": ("records", "sum"),
        "numeric_pressure": ("numeric_pressure", "sum"),
        "pressure_sum": ("pressure_sum", "sum"),
        "pressure_min": ("pressure_min", "min"),
        "pressure_max": ("pressure_max", "max"),
    }

    for category in CATEGORIES:
        aggregations[f"count_{category}"] = (
            f"count_{category}",
            "sum",
        )

    result = (
        combined.groupby(["sensor_id", "date"])
        .agg(**aggregations)
        .reset_index()
    )

    result["pressure_mean"] = (
        result["pressure_sum"]
        / result["numeric_pressure"].replace(0, np.nan)
    )

    result = result.drop(columns=["pressure_sum"])
    result = result.sort_values(["sensor_id", "date"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print("\nPressure timeline analysis completed.")
    print(f"Report: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()