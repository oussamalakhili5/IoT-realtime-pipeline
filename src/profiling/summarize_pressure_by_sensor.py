
"""
BME280 Pressure Unit Investigation.

This script analyzes pressure values by sensor to investigate
possible inconsistencies between hectopascals (hPa) and pascals (Pa).

It processes the historical CSV in chunks and generates a per-sensor
summary without modifying the source dataset.

The pressure ranges are investigative heuristics, not final
data-cleaning rules.
"""

from pathlib import Path

import pandas as pd


DATASET = Path(
    "data/raw/2026-03_bme280/2026-03_bme280.csv"
)
OUTPUT = Path("reports/pressure_by_sensor.csv")

CHUNK_SIZE = 100_000

# Define candidate ranges for investigating possible pressure units.
HPA_RANGE = (300.0, 1200.0)
PA_RANGE = (30_000.0, 110_000.0)


def main():
    """Summarize pressure values and candidate units for each sensor."""

    if not DATASET.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET}"
        )

    # Keep cumulative statistics for each sensor across all chunks.
    sensor_stats = {}
    total_rows = 0

    print("Starting per-sensor pressure investigation...", flush=True)

    # Read the dataset incrementally to limit memory consumption.
    for chunk in pd.read_csv(
        DATASET,
        sep=";",
        chunksize=CHUNK_SIZE,
        dtype={
            "sensor_id": "string",
            "pressure": "string",
        },
    ):
        # Convert pressure values to numeric values for analysis.
        pressure = pd.to_numeric(
            chunk["pressure"],
            errors="coerce",
        )

        # Classify values using the candidate hPa and Pa ranges.
        hpa_like = pressure.between(
            HPA_RANGE[0], HPA_RANGE[1]
        )
        pa_like = pressure.between(
            PA_RANGE[0], PA_RANGE[1]
        )

        numeric = pressure.notna()
        other = numeric & ~(hpa_like | pa_like)

        # Group records by sensor to identify sensor-specific patterns.
        for sensor_id, indices in chunk.groupby(
            "sensor_id",
            sort=False,
            dropna=False,
        ).groups.items():
            sensor_key = (
                str(sensor_id)
                if pd.notna(sensor_id)
                else "<MISSING_SENSOR_ID>"
            )

            # Obtain pressure values for this sensor in the current chunk.
            values = pressure.loc[indices]
            hpa_values = values[values.between(*HPA_RANGE)]
            pa_values = values[values.between(*PA_RANGE)]
            other_values = values[
                values.notna()
                & ~values.between(*HPA_RANGE)
                & ~values.between(*PA_RANGE)
            ]

            # Initialize cumulative statistics for previously unseen sensors.
            stats = sensor_stats.setdefault(
                sensor_key,
                {
                    "records": 0,
                    "pressure_numeric": 0,
                    "pressure_missing_or_non_numeric": 0,
                    "pressure_hpa_like": 0,
                    "pressure_pa_like": 0,
                    "pressure_other": 0,
                    "pressure_min": None,
                    "pressure_max": None,
                    "sample_hpa_like": None,
                    "sample_pa_like": None,
                    "sample_other": None,
                },
            )

            # Count records and pressure categories for this sensor.
            stats["records"] += len(indices)
            stats["pressure_numeric"] += int(values.notna().sum())
            stats["pressure_missing_or_non_numeric"] += int(
                values.isna().sum()
            )
            stats["pressure_hpa_like"] += int(hpa_values.size)
            stats["pressure_pa_like"] += int(pa_values.size)
            stats["pressure_other"] += int(other_values.size)

            # Update minimum and maximum using numeric values only.
            valid_values = values.dropna()

            if not valid_values.empty:
                current_min = float(valid_values.min())
                current_max = float(valid_values.max())

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

            # Retain one example from each category for inspection.
            if (
                stats["sample_hpa_like"] is None
                and not hpa_values.empty
            ):
                stats["sample_hpa_like"] = float(
                    hpa_values.iloc[0]
                )

            if (
                stats["sample_pa_like"] is None
                and not pa_values.empty
            ):
                stats["sample_pa_like"] = float(
                    pa_values.iloc[0]
                )

            if (
                stats["sample_other"] is None
                and not other_values.empty
            ):
                stats["sample_other"] = float(
                    other_values.iloc[0]
                )

        total_rows += len(chunk)

        # Display progress every two million records.
        if total_rows % (CHUNK_SIZE * 20) == 0:
            print(f"Processed {total_rows:,} rows...", flush=True)

    # Convert accumulated statistics into a tabular report.
    result = pd.DataFrame.from_dict(
        sensor_stats,
        orient="index",
    )
    result.index.name = "sensor_id"
    result = result.reset_index()

    # Flag sensors that contain values in both candidate unit ranges.
    result["has_both_candidate_units"] = (
        (result["pressure_hpa_like"] > 0)
        & (result["pressure_pa_like"] > 0)
    )

    # Sort sensors with the most hPa-like observations first.
    result = result.sort_values(
        by="pressure_hpa_like",
        ascending=False,
    )

    # Save the report for further investigation.
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT, index=False)

    # Print a concise summary to the terminal.
    print("\n=== PRESSURE UNIT INVESTIGATION ===")
    print(f"Total records: {total_rows:,}")
    print(f"Distinct sensors: {len(result):,}")
    print(
        "Sensors with hPa-like values:",
        int((result["pressure_hpa_like"] > 0).sum()),
    )
    print(
        "Sensors with Pa-like values:",
        int((result["pressure_pa_like"] > 0).sum()),
    )
    print(
        "Sensors with both candidate units:",
        int(result["has_both_candidate_units"].sum()),
    )
    print(f"\nReport saved to: {OUTPUT}")


if __name__ == "__main__":
    main()
