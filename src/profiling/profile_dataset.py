"""
BME280 IoT dataset profiling.

This script analyzes a CSV file in chunks to:
- Count the total number of rows and missing values.
- Identify distinct sensor IDs.
- Detect invalid timestamps.
- Find the earliest and latest timestamps.
- Calculate minimum and maximum values for numerical measurements.
- Generate a report in the reports/ directory.
"""

from pathlib import Path
import pandas as pd


# Path to the dataset to analyze.
DATASET = Path(
    "data/raw/2026-03_bme280/2026-03_bme280.csv"
)

# Number of rows processed in each chunk to reduce memory usage.
CHUNK_SIZE = 100_000


def main():
    """Profile the dataset and save the results to a report."""

    # Check whether the CSV file exists.
    if not DATASET.is_file():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET}"
        )

    # Initialize variables used to track dataset statistics.
    total_rows = 0
    missing_values = {}       # Number of missing values per column.
    sensor_ids = set()        # Unique sensor identifiers.
    invalid_timestamps = 0    # Number of invalid timestamps.
    timestamp_min = None      # Earliest valid timestamp.
    timestamp_max = None      # Latest valid timestamp.
    numeric_stats = {}        # Minimum and maximum values per measurement.

    print("Starting dataset profiling...")

    # Read the CSV in chunks instead of loading the entire dataset into memory.
    for chunk in pd.read_csv(
    DATASET,
    sep=";",
    chunksize=CHUNK_SIZE,
    dtype={"pressure": "string"},
    ):
        # Update the total number of rows processed.
        total_rows += len(chunk)

        # Count missing values in each column.
        for column, count in chunk.isna().sum().items():
            missing_values[column] = (
                missing_values.get(column, 0) + int(count)
            )

        # Collect unique sensor identifiers.
        if "sensor_id" in chunk.columns:
            sensor_ids.update(
                chunk["sensor_id"]
                .dropna()        # Ignore missing sensor IDs.
                .astype(str)     # Convert IDs to strings.
                .unique()        # Keep distinct IDs within the chunk.
            )

        # Validate timestamps and identify invalid values.
        if "timestamp" in chunk.columns:
            timestamps = pd.to_datetime(
                chunk["timestamp"],
                format="ISO8601",
                errors="coerce",  # Invalid timestamps become NaT.
            )

            # Count invalid or missing timestamps.
            invalid_timestamps += int(timestamps.isna().sum())

            # Keep only valid timestamps.
            valid = timestamps.dropna()

            if not valid.empty:
                t_min = valid.min()
                t_max = valid.max()

                # Update the earliest timestamp found so far.
                if timestamp_min is None or t_min < timestamp_min:
                    timestamp_min = t_min

                # Update the latest timestamp found so far.
                if timestamp_max is None or t_max > timestamp_max:
                    timestamp_max = t_max

        # Define the numerical columns to profile.
        for column in [
            "temperature",
            "humidity",
            "pressure",
            "lat",
            "lon",
        ]:
            # Skip columns that do not exist in the dataset.
            if column not in chunk.columns:
                continue

            # Convert values to numbers.
            # Values that cannot be converted become NaN.
            values = pd.to_numeric(
                chunk[column],
                errors="coerce",
            ).dropna()

            # Skip columns with no valid numerical values.
            if values.empty:
                continue

            # Calculate the minimum and maximum for the current chunk.
            current_min = values.min()
            current_max = values.max()

            # Initialize statistics for the column if necessary.
            if column not in numeric_stats:
                numeric_stats[column] = [
                    current_min,
                    current_max,
                ]
            else:
                # Update the global minimum and maximum across all chunks.
                numeric_stats[column][0] = min(
                    numeric_stats[column][0], current_min
                )
                numeric_stats[column][1] = max(
                    numeric_stats[column][1], current_max
                )

    # Display the general dataset profile.
    print("\n=== DATASET PROFILE ===")
    print(f"Total number of rows: {total_rows:,}")
    print(f"Number of columns: {len(missing_values)}")
    print(f"Number of distinct sensors: {len(sensor_ids)}")
    print(f"Invalid timestamps: {invalid_timestamps}")
    print(f"Earliest timestamp: {timestamp_min}")
    print(f"Latest timestamp: {timestamp_max}")

    # Display missing values and their percentages.
    print("\n=== MISSING VALUES ===")
    for column, count in missing_values.items():
        percentage = 100 * count / total_rows if total_rows else 0
        print(f"{column}: {count:,} ({percentage:.2f}%)")

    # Display minimum and maximum values for numerical measurements.
    print("\n=== MINIMUM AND MAXIMUM VALUES ===")
    for column, (minimum, maximum) in numeric_stats.items():
        print(f"{column}: min={minimum}, max={maximum}")

    # Define the output report path.
    report = Path("reports/dataset_profile.txt")

    # Create the reports/ directory if it does not exist.
    report.parent.mkdir(parents=True, exist_ok=True)

    # Write the profiling results to a text file.
    with report.open("w", encoding="utf-8") as file:
        file.write(f"Total rows: {total_rows}\n")
        file.write(f"Distinct sensors: {len(sensor_ids)}\n")
        file.write(f"Invalid timestamps: {invalid_timestamps}\n")
        file.write(f"Earliest timestamp: {timestamp_min}\n")
        file.write(f"Latest timestamp: {timestamp_max}\n")

        # Write missing-value counts for each column.
        file.write("\nMissing values:\n")
        for column, count in missing_values.items():
            file.write(f"{column}: {count}\n")

        # Write minimum and maximum values for numerical measurements.
        file.write("\nNumerical statistics:\n")
        for column, stats in numeric_stats.items():
            file.write(
                f"{column}: min={stats[0]}, max={stats[1]}\n"
            )

    print(f"\nReport saved to: {report}")


# Run main() only when this script is executed directly.
if __name__ == "__main__":
    main()
