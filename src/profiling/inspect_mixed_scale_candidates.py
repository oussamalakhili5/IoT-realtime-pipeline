
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from src.streaming.pressure_rules import normalize_pressure


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")
DIAGNOSIS_PATH = Path("reports/pressure_unit_diagnosis_by_sensor.csv")

CANDIDATES_PATH = Path("reports/mixed_scale_pressure_candidates.csv")
TIMELINE_PATH = Path("reports/mixed_scale_pressure_timeline.csv")

CHUNK_SIZE = 500_000

STATUSES = [
    "missing_or_nonnumeric",
    "invalid_nonpositive",
    "converted_from_hpa",
    "converted_from_hpa_out_of_range",
    "valid_pa",
    "outside_candidate_ranges",
]

HPA_STATUSES = {
    "converted_from_hpa",
    "converted_from_hpa_out_of_range",
}


def main() -> None:
    """Inspect candidate hPa readings from provisionally mixed sensors."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    if not DIAGNOSIS_PATH.exists():
        raise FileNotFoundError(
            f"Diagnosis report not found: {DIAGNOSIS_PATH}"
        )

    diagnosis = pd.read_csv(
        DIAGNOSIS_PATH,
        dtype={"sensor_id": "string"},
    )

    target_sensor_ids = set(
        diagnosis.loc[
            diagnosis["unit_diagnosis"] == "mixed_scale_candidates",
            "sensor_id",
        ].dropna()
    )

    if not target_sensor_ids:
        raise ValueError("No mixed-scale candidate sensors were found.")

    daily_stats = {}
    candidate_records = []
    total_rows_scanned = 0

    columns = [
        "sensor_id",
        "timestamp",
        "pressure",
        "temperature",
        "humidity",
        "lat",
        "lon",
    ]

    reader = pd.read_csv(
        DATA_PATH,
        sep=";",
        usecols=columns,
        dtype={"sensor_id": "string"},
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows_scanned += len(chunk)

        relevant = chunk[
            chunk["sensor_id"].isin(target_sensor_ids)
        ].copy()

        if relevant.empty:
            continue

        relevant["timestamp_clean"] = (
            relevant["timestamp"]
            .astype("string")
            .str.strip()
            .str.strip("'\"")
        )

        parsed_timestamp = pd.to_datetime(
            relevant["timestamp_clean"],
            format="ISO8601",
            errors="coerce",
        )

        relevant["date"] = (
            parsed_timestamp.dt.strftime("%Y-%m-%d")
            .fillna("INVALID_TIMESTAMP")
        )

        relevant["quality_status"] = [
            normalize_pressure(value)[2]
            for value in relevant["pressure"]
        ]

        relevant["pressure_numeric"] = pd.to_numeric(
            relevant["pressure"],
            errors="coerce",
        )

        candidate_mask = relevant["quality_status"].isin(
            HPA_STATUSES
        )

        candidate_columns = [
            "sensor_id",
            "timestamp_clean",
            "date",
            "pressure",
            "quality_status",
            "temperature",
            "humidity",
            "lat",
            "lon",
        ]

        if candidate_mask.any():
            candidates = relevant.loc[
                candidate_mask, candidate_columns
            ].copy()

            candidates = candidates.rename(
                columns={
                    "timestamp_clean": "timestamp",
                    "pressure": "pressure_raw",
                }
            )

            candidate_records.extend(
                candidates.to_dict(orient="records")
            )

        for (sensor_id, date), group in relevant.groupby(
            ["sensor_id", "date"]
        ):
            key = (str(sensor_id), str(date))

            stats = daily_stats.setdefault(
                key,
                {
                    "sensor_id": str(sensor_id),
                    "date": str(date),
                    "records": 0,
                    "counts": Counter(),
                    "pressure_min": None,
                    "pressure_max": None,
                },
            )

            stats["records"] += len(group)

            counts = group["quality_status"].value_counts()

            stats["counts"].update(
                {
                    str(status): int(count)
                    for status, count in counts.items()
                }
            )

            numeric = pd.to_numeric(
                group["pressure"], errors="coerce"
            )

            numeric = numeric[
                np.isfinite(
                    numeric.to_numpy(
                        dtype=float,
                        na_value=np.nan,
                    )
                )
            ]

            if not numeric.empty:
                current_min = float(numeric.min())
                current_max = float(numeric.max())

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

        if chunk_number % 50 == 0:
            print(
                f"Processed {chunk_number} chunks "
                f"({total_rows_scanned:,} rows)..."
            )

    CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)

    candidate_columns = [
        "sensor_id",
        "timestamp",
        "date",
        "pressure_raw",
        "quality_status",
        "temperature",
        "humidity",
        "lat",
        "lon",
    ]

    candidate_df = pd.DataFrame(
        candidate_records,
        columns=candidate_columns,
    )

    if not candidate_df.empty:
        candidate_df = candidate_df.sort_values(
            ["sensor_id", "timestamp"]
        )

    candidate_df.to_csv(CANDIDATES_PATH, index=False)

    timeline_records = []

    for stats in daily_stats.values():
        record = {
            "sensor_id": stats["sensor_id"],
            "date": stats["date"],
            "records": stats["records"],
            "pressure_min": stats["pressure_min"],
            "pressure_max": stats["pressure_max"],
        }

        for status in STATUSES:
            record[f"{status}_count"] = stats["counts"].get(
                status, 0
            )

        record["hpa_candidate_count"] = (
            record["converted_from_hpa_count"]
            + record["converted_from_hpa_out_of_range_count"]
        )

        record["pa_candidate_count"] = record["valid_pa_count"]

        timeline_records.append(record)

    timeline_df = pd.DataFrame(timeline_records)

    if not timeline_df.empty:
        timeline_df = timeline_df.sort_values(
            ["sensor_id", "date"]
        )

    timeline_df.to_csv(TIMELINE_PATH, index=False)

    print("\nMixed-scale candidate investigation completed.")
    print(f"Target sensors: {len(target_sensor_ids)}")
    print(f"Candidate hPa-range records: {len(candidate_df):,}")
    print(f"Timeline rows: {len(timeline_df):,}")
    print(f"Candidate records: {CANDIDATES_PATH}")
    print(f"Daily timeline: {TIMELINE_PATH}")


if __name__ == "__main__":
    main()
