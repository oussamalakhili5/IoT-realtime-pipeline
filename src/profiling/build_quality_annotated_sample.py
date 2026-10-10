
"""Build a quality-annotated sample without modifying raw data."""

from pathlib import Path

import pandas as pd

from src.streaming.measurement_rules import assess_measurement


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "reports" / "pressure_record_samples.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "interim" / "quality_annotated_sample.csv"


def main():
    """Annotate sample measurements with data quality statuses."""

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    required_columns = {
        "pressure_numeric",
        "temperature",
        "humidity",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}. "
            f"Available columns: {list(df.columns)}"
        )

    quality_results = df.apply(
        lambda row: assess_measurement(
            pressure=row["pressure_numeric"],
            temperature=row["temperature"],
            humidity=row["humidity"],
        ),
        axis=1,
    )

    quality_df = pd.DataFrame(
        quality_results.tolist(),
        index=df.index,
    )

    annotated_df = pd.concat([df, quality_df], axis=1)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    annotated_df.to_csv(OUTPUT_FILE, index=False)

    print("=" * 60)
    print("QUALITY ANNOTATION COMPLETED")
    print("=" * 60)

    print(f"\nInput file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Number of records: {len(annotated_df):,}")
    print(f"Number of columns: {len(annotated_df.columns)}")

    print("\nRecord status summary:")
    print(
        annotated_df["record_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nPressure status summary:")
    print(
        annotated_df["pressure_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nTemperature status summary:")
    print(
        annotated_df["temperature_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nHumidity status summary:")
    print(
        annotated_df["humidity_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    print("\nFirst 10 annotated records:")
    print(annotated_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
