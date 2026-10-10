
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")
SUMMARY_PATH = Path("reports/pressure_sensor_inspection.csv")
SAMPLES_PATH = Path("reports/pressure_record_samples.csv")

CHUNK_SIZE = 100_000
SAMPLES_PER_SIDE = 5

TARGET_SENSOR_IDS = {"39287", "93517", "54381", "78677"}

PRESSURE_CATEGORIES = [
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
    """Inspect pressure records for selected representative sensors."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}. "
            "Check the file path before running the script."
        )

    summary = {
        sensor_id: {
            "sensor_id": sensor_id,
            "records": 0,
            "numeric_pressure": 0,
            "missing_or_nonnumeric": 0,
            "zero_or_negative": 0,
            "hPa_like": 0,
            "Pa_like": 0,
            "outside_candidate_ranges": 0,
            "pressure_min": None,
            "pressure_max": None,
            "first_timestamp": None,
            "last_timestamp": None,
        }
        for sensor_id in TARGET_SENSOR_IDS
    }

    sample_buckets = {}

    columns = [
        "sensor_id",
        "timestamp",
        "pressure",
        "temperature",
        "humidity",
    ]

    for chunk_number, chunk in enumerate(
        pd.read_csv(
            DATA_PATH,
            sep=";",
            usecols=columns,
            dtype={"sensor_id": "string"},
            chunksize=CHUNK_SIZE,
            low_memory=False,
        ),
        start=1,
    ):
        relevant = chunk[
            chunk["sensor_id"].isin(TARGET_SENSOR_IDS)
        ].copy()

        if relevant.empty:
            continue

        relevant["timestamp_clean"] = (
            relevant["timestamp"]
            .astype("string")
            .str.strip()
            .str.strip("'\"")
        )

        relevant["pressure_numeric"] = pd.to_numeric(
            relevant["pressure"], errors="coerce"
        )
        relevant["temperature_numeric"] = pd.to_numeric(
            relevant["temperature"], errors="coerce"
        )
        relevant["humidity_numeric"] = pd.to_numeric(
            relevant["humidity"], errors="coerce"
        )

        relevant["pressure_category"] = classify_pressure(
            relevant["pressure_numeric"]
        )

        for sensor_id, group in relevant.groupby("sensor_id"):
            sensor_id = str(sensor_id)
            stats = summary[sensor_id]

            pressure = group["pressure_numeric"]
            stats["records"] += len(group)
            stats["numeric_pressure"] += int(pressure.notna().sum())

            for category in PRESSURE_CATEGORIES:
                count = int(
                    (group["pressure_category"] == category).sum()
                )
                stats[category] += count

            valid_pressure = pressure.dropna()
            if not valid_pressure.empty:
                current_min = float(valid_pressure.min())
                current_max = float(valid_pressure.max())

                if stats["pressure_min"] is None:
                    stats["pressure_min"] = current_min
                    stats["pressure_max"] = current_max
                else:
                    stats["pressure_min"] = min(
                        stats["pressure_min"], current_min
                    )
                    stats["pressure_max"] = max(
                        stats["pressure_max"], current_max
                    )

            timestamps = group["timestamp_clean"].dropna()
            if not timestamps.empty:
                current_first = str(timestamps.min())
                current_last = str(timestamps.max())

                if stats["first_timestamp"] is None:
                    stats["first_timestamp"] = current_first
                    stats["last_timestamp"] = current_last
                else:
                    stats["first_timestamp"] = min(
                        stats["first_timestamp"], current_first
                    )
                    stats["last_timestamp"] = max(
                        stats["last_timestamp"], current_last
                    )

            for category in PRESSURE_CATEGORIES:
                category_rows = group[
                    group["pressure_category"] == category
                ]

                if category_rows.empty:
                    continue

                key = (sensor_id, category)
                bucket = sample_buckets.setdefault(
                    key,
                    {
                        "first": [],
                        "last": deque(maxlen=SAMPLES_PER_SIDE),
                    },
                )

                for row in category_rows.itertuples(index=False):
                    sample = {
                        "sensor_id": sensor_id,
                        "timestamp": row.timestamp_clean,
                        "pressure_raw": row.pressure,
                        "pressure_numeric": row.pressure_numeric,
                        "pressure_category": category,
                        "temperature": row.temperature_numeric,
                        "humidity": row.humidity_numeric,
                    }

                    if len(bucket["first"]) < SAMPLES_PER_SIDE:
                        bucket["first"].append(sample)

                    bucket["last"].append(sample)

        if chunk_number % 50 == 0:
            print(f"Processed {chunk_number} chunks...")

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(summary.values()).sort_values("sensor_id").to_csv(
        SUMMARY_PATH, index=False
    )

    sample_records = []

    for (sensor_id, category), bucket in sample_buckets.items():
        for sample in bucket["first"]:
            sample_records.append(
                {**sample, "sample_position": "first_in_file"}
            )

        for sample in bucket["last"]:
            sample_records.append(
                {**sample, "sample_position": "last_in_file"}
            )

    pd.DataFrame(sample_records).to_csv(SAMPLES_PATH, index=False)

    print("\nPressure inspection completed.")
    print(f"Sensor summary: {SUMMARY_PATH}")
    print(f"Raw record samples: {SAMPLES_PATH}")


if __name__ == "__main__":
    main()
