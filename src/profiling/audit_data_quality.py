"""
BME280 IoT Dataset - Data Quality Audit.

This script scans a large CSV dataset in chunks to reduce memory usage.

The audit checks:
- Missing values in each column.
- Invalid timestamps.
- Non-numeric values in measurement columns.
- Values outside configured screening ranges.
- Representative examples of problematic records.

The original dataset is never modified.
"""

from pathlib import Path

import pandas as pd


# Define the path to the original BME280 dataset.
DATASET = Path(
    "data/raw/2026-03_bme280/2026-03_bme280.csv"
)

# Process 100,000 records at a time to limit memory consumption.
CHUNK_SIZE = 100_000

# Store up to five example records for each type of issue.
SAMPLE_LIMIT = 5

# Define initial validation ranges for measurements.
# Pressure is assumed to be expressed in pascals (Pa).
# These are screening rules, not proof of sensor failure.
RANGES = {
    "temperature": (-40.0, 85.0),
    "humidity": (0.0, 100.0),
    "pressure": (30_000.0, 110_000.0),
    "lat": (-90.0, 90.0),
    "lon": (-180.0, 180.0),
}


def main():
    """Audit data quality and save reports without changing source data."""

    # Stop immediately if the source dataset cannot be found.
    if not DATASET.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET}"
        )

    # Track the total number of records processed.
    total_rows = 0

    # Store cumulative missing-value counts for every column.
    missing_counts = {}

    # Store the number of non-empty values that cannot be parsed as numbers.
    parse_failures = {}

    # Initialize the number of values outside each screening range.
    out_of_range = {
        column: 0 for column in RANGES
    }

    # Count timestamps that cannot be parsed as valid dates.
    invalid_timestamps = 0

    # Keep a limited number of representative problematic records.
    samples = []

    # Track how many examples have been collected for each issue type.
    sample_counts = {}

    print("Starting data quality audit...", flush=True)

    # Read the CSV incrementally instead of loading the entire file into RAM.
    # Read pressure as text because the source contains mixed data types.
    for chunk in pd.read_csv(
        DATASET,
        sep=";",
        chunksize=CHUNK_SIZE,
        dtype={"pressure": "string"},
    ):
        # Count missing values in the current chunk.
        for column, count in chunk.isna().sum().items():
            missing_counts[column] = (
                missing_counts.get(column, 0) + int(count)
            )

        # Validate timestamps using ISO 8601 parsing.
        if "timestamp" in chunk.columns:
            timestamps = pd.to_datetime(
                chunk["timestamp"],
                format="ISO8601",
                errors="coerce",
            )

            # Invalid or unparseable timestamps become NaT.
            invalid_mask = timestamps.isna()

            # Add the invalid timestamp count to the running total.
            invalid_timestamps += int(invalid_mask.sum())

            # Collect a few examples to help investigate timestamp issues.
            issue = "invalid_timestamp"
            remaining = (
                SAMPLE_LIMIT - sample_counts.get(issue, 0)
            )

            invalid_positions = (
                invalid_mask.to_numpy().nonzero()[0]
            )

            for pos in invalid_positions[:max(0, remaining)]:
                row = chunk.iloc[pos]

                # The CSV row number includes the header row.
                samples.append({
                    "csv_row": total_rows + int(pos) + 2,
                    "issue": issue,
                    "sensor_id": row.get("sensor_id"),
                    "timestamp": row.get("timestamp"),
                    "column": "timestamp",
                    "value": row.get("timestamp"),
                })

                sample_counts[issue] = (
                    sample_counts.get(issue, 0) + 1
                )

        # Validate numeric values and check configured screening ranges.
        for column, (lower, upper) in RANGES.items():

            # Skip columns that are not present in the source dataset.
            if column not in chunk.columns:
                continue

            raw = chunk[column]

            # Convert values to numbers.
            # Values that cannot be converted become NaN.
            numeric = pd.to_numeric(
                raw,
                errors="coerce",
            )

            # Identify values that are neither missing nor empty strings.
            # This prevents genuinely missing cells from being counted
            # as non-numeric parsing failures.
            non_empty = (
                raw.notna()
                & raw.astype("string")
                .str.strip()
                .ne("")
                .fillna(False)
            )

            # Count non-empty values that cannot be converted to numbers.
            failed = non_empty & numeric.isna()

            parse_failures[column] = (
                parse_failures.get(column, 0)
                + int(failed.sum())
            )

            # Flag numeric values outside the configured screening range.
            invalid_range = numeric.notna() & (
                (numeric < lower) | (numeric > upper)
            )

            # Accumulate the number of range violations across all chunks.
            out_of_range[column] += int(
                invalid_range.sum()
            )

            # Collect a small sample of range violations for investigation.
            issue = f"{column}_outside_screening_range"
            remaining = (
                SAMPLE_LIMIT - sample_counts.get(issue, 0)
            )

            invalid_positions = (
                invalid_range.to_numpy().nonzero()[0]
            )

            for pos in invalid_positions[:max(0, remaining)]:
                row = chunk.iloc[pos]

                samples.append({
                    "csv_row": total_rows + int(pos) + 2,
                    "issue": issue,
                    "sensor_id": row.get("sensor_id"),
                    "timestamp": row.get("timestamp"),
                    "column": column,
                    "value": row.get(column),
                })

                sample_counts[issue] = (
                    sample_counts.get(issue, 0) + 1
                )

        # Update the total number of records processed.
        total_rows += len(chunk)

        # Display progress every two million processed records.
        if total_rows % (CHUNK_SIZE * 20) == 0:
            print(
                f"Processed {total_rows:,} rows...",
                flush=True,
            )

    # Define the output path for the text audit report.
    report_path = Path("reports/data_quality_audit.txt")

    # Create the reports directory if it does not exist.
    report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Write the audit results to a human-readable text file.
    with report_path.open("w", encoding="utf-8") as report:
        report.write("BME280 DATA QUALITY AUDIT\n")
        report.write("=========================\n\n")
        report.write(f"Dataset: {DATASET}\n")
        report.write(f"Total records: {total_rows:,}\n\n")

        # Report the number and percentage of missing values per column.
        report.write("MISSING VALUES\n")

        for column, count in missing_counts.items():
            percentage = (
                100 * count / total_rows
                if total_rows
                else 0
            )

            report.write(
                f"{column}: {count:,} ({percentage:.4f}%)\n"
            )

        # Report the total number of invalid timestamps.
        report.write(
            f"\nInvalid timestamps: {invalid_timestamps:,}\n"
        )

        # Report non-empty values that failed numeric conversion.
        report.write("\nNON-NUMERIC VALUES\n")

        for column, count in parse_failures.items():
            report.write(f"{column}: {count:,}\n")

        # Report values outside the configured screening ranges.
        report.write("\nVALUES OUTSIDE SCREENING RANGES\n")

        for column, count in out_of_range.items():
            lower, upper = RANGES[column]

            report.write(
                f"{column} [{lower}, {upper}]: {count:,}\n"
            )

    # Save representative problematic records as a separate CSV file.
    samples_path = Path("reports/data_quality_samples.csv")

    pd.DataFrame(samples).to_csv(
        samples_path,
        index=False,
    )

    # Print a summary of the audit results to the terminal.
    print("\n=== DATA QUALITY AUDIT ===")
    print(f"Total records: {total_rows:,}")
    print(f"Invalid timestamps: {invalid_timestamps:,}")

    print("\nMissing values:")
    for column, count in missing_counts.items():
        print(f"  {column}: {count:,}")

    print("\nNon-numeric values:")
    for column, count in parse_failures.items():
        print(f"  {column}: {count:,}")

    print("\nValues outside screening ranges:")
    for column, count in out_of_range.items():
        print(f"  {column}: {count:,}")

    print(f"\nReport saved to: {report_path}")
    print(f"Sample records saved to: {samples_path}")


# Execute the audit only when this file is run directly.
if __name__ == "__main__":
    main()