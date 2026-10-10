
"""
BME280 Data Quality Investigation.

This script investigates suspicious values identified by the
initial data quality audit. It checks timestamp formatting,
records outside March 2026, possible pressure units, and
extreme temperature and humidity values.

The source dataset is read-only and is never modified.
"""

from pathlib import Path

import pandas as pd


DATASET = Path(
    "data/raw/2026-03_bme280/2026-03_bme280.csv"
)

CHUNK_SIZE = 100_000
SAMPLE_LIMIT = 5

MONTH_START = pd.Timestamp("2026-03-01 00:00:00")
NEXT_MONTH_START = pd.Timestamp("2026-04-01 00:00:00")


def collect_samples(mask, issue, chunk, row_offset, counts, samples):
    """Store a limited number of representative records for each issue."""

    remaining = SAMPLE_LIMIT - counts.get(issue, 0)

    if remaining <= 0:
        return

    # Identify matching positions without copying the entire chunk.
    positions = mask.fillna(False).to_numpy().nonzero()[0]

    for position in positions[:remaining]:
        row = chunk.iloc[position]

        samples.append({
            "csv_row": row_offset + int(position) + 2,
            "issue": issue,
            "sensor_id": row.get("sensor_id"),
            "timestamp": row.get("timestamp"),
            "pressure": row.get("pressure"),
            "temperature": row.get("temperature"),
            "humidity": row.get("humidity"),
        })

        counts[issue] = counts.get(issue, 0) + 1


def main():
    """Investigate timestamp and measurement anomalies in CSV chunks."""

    if not DATASET.is_file():
        raise FileNotFoundError(f"Dataset not found: {DATASET}")

    counts = {
        "rows": 0,
        "timestamps_with_quotes": 0,
        "timestamps_invalid_before_cleanup": 0,
        "timestamps_invalid_after_cleanup": 0,
        "timestamps_repaired": 0,
        "timestamps_outside_march": 0,
        "pressure_hpa_like": 0,
        "pressure_pa_like": 0,
        "pressure_outside_both_ranges": 0,
        "temperature_below_range": 0,
        "temperature_above_range": 0,
        "humidity_below_range": 0,
        "humidity_above_range": 0,
    }

    samples = []
    sample_counts = {}

    print("Starting data quality investigation...", flush=True)

    for chunk in pd.read_csv(
        DATASET,
        sep=";",
        chunksize=CHUNK_SIZE,
        dtype={
            "timestamp": "string",
            "pressure": "string",
        },
    ):
        row_offset = counts["rows"]

        # Inspect timestamps before and after removing surrounding quotes.
        raw_timestamp = chunk["timestamp"].astype("string").str.strip()

        quoted = (
            raw_timestamp.str.startswith("'").fillna(False)
            | raw_timestamp.str.endswith("'").fillna(False)
            | raw_timestamp.str.startswith('"').fillna(False)
            | raw_timestamp.str.endswith('"').fillna(False)
        )

        counts["timestamps_with_quotes"] += int(quoted.sum())

        original_parsed = pd.to_datetime(
            raw_timestamp,
            format="ISO8601",
            errors="coerce",
        )

        cleaned_timestamp = raw_timestamp.str.strip("'\"")

        cleaned_parsed = pd.to_datetime(
            cleaned_timestamp,
            format="ISO8601",
            errors="coerce",
        )

        original_invalid = original_parsed.isna()
        cleaned_invalid = cleaned_parsed.isna()

        counts["timestamps_invalid_before_cleanup"] += int(
            original_invalid.sum()
        )

        counts["timestamps_invalid_after_cleanup"] += int(
            cleaned_invalid.sum()
        )

        repaired = original_invalid & cleaned_parsed.notna()

        counts["timestamps_repaired"] += int(repaired.sum())

        # Identify timestamps outside the requested month.
        outside_march = (
            cleaned_parsed.notna()
            & (
                (cleaned_parsed < MONTH_START)
                | (cleaned_parsed >= NEXT_MONTH_START)
            )
        )

        counts["timestamps_outside_march"] += int(
            outside_march.sum()
        )

        collect_samples(
            cleaned_invalid,
            "timestamp_still_invalid",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        collect_samples(
            outside_march,
            "timestamp_outside_march",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        # Convert pressure values to numbers for unit-range investigation.
        pressure = pd.to_numeric(
            chunk["pressure"],
            errors="coerce",
        )

        # These ranges help distinguish hPa-like and Pa-like values.
        # They are investigation categories, not final cleaning rules.
        hpa_like = pressure.between(300, 1200, inclusive="both")
        pa_like = pressure.between(30_000, 110_000, inclusive="both")

        pressure_numeric = pressure.notna()

        outside_pressure_ranges = (
            pressure_numeric & ~(hpa_like | pa_like)
        )

        counts["pressure_hpa_like"] += int(hpa_like.sum())
        counts["pressure_pa_like"] += int(pa_like.sum())
        counts["pressure_outside_both_ranges"] += int(
            outside_pressure_ranges.sum()
        )

        collect_samples(
            hpa_like,
            "pressure_hpa_like",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        collect_samples(
            outside_pressure_ranges,
            "pressure_outside_both_ranges",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        # Investigate temperatures outside the configured screening range.
        temperature = pd.to_numeric(
            chunk["temperature"],
            errors="coerce",
        )

        temperature_below = temperature < -40
        temperature_above = temperature > 85

        counts["temperature_below_range"] += int(
            temperature_below.sum()
        )

        counts["temperature_above_range"] += int(
            temperature_above.sum()
        )

        collect_samples(
            temperature_below,
            "temperature_below_range",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        collect_samples(
            temperature_above,
            "temperature_above_range",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        # Investigate humidity values outside the relative-humidity range.
        humidity = pd.to_numeric(
            chunk["humidity"],
            errors="coerce",
        )

        humidity_below = humidity < 0
        humidity_above = humidity > 100

        counts["humidity_below_range"] += int(
            humidity_below.sum()
        )

        counts["humidity_above_range"] += int(
            humidity_above.sum()
        )

        collect_samples(
            humidity_below,
            "humidity_below_range",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        collect_samples(
            humidity_above,
            "humidity_above_range",
            chunk,
            row_offset,
            sample_counts,
            samples,
        )

        counts["rows"] += len(chunk)

        if counts["rows"] % (CHUNK_SIZE * 20) == 0:
            print(
                f"Processed {counts['rows']:,} rows...",
                flush=True,
            )

    # Save investigation statistics and representative examples.
    report_path = Path("reports/data_quality_investigation.txt")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with report_path.open("w", encoding="utf-8") as report:
        report.write("BME280 DATA QUALITY INVESTIGATION\n")
        report.write("=================================\n\n")

        for name, value in counts.items():
            report.write(f"{name}: {value:,}\n")

    samples_path = Path(
        "reports/data_quality_investigation_samples.csv"
    )

    pd.DataFrame(samples).to_csv(
        samples_path,
        index=False,
    )

    print("\n=== INVESTIGATION RESULTS ===")

    for name, value in counts.items():
        print(f"{name}: {value:,}")

    print(f"\nReport saved to: {report_path}")
    print(f"Sample records saved to: {samples_path}")


if __name__ == "__main__":
    main()

