
from pathlib import Path

import pandas as pd

from src.streaming.measurement_rules import assess_measurement


INPUT_PATH = Path("reports/pressure_record_samples.csv")

OUTPUT_PATH = Path("reports/measurement_rules_validation.csv")
SUMMARY_PATH = Path("reports/measurement_rules_validation_summary.csv")


def main() -> None:
    """Validate measurement quality rules on representative raw records."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Sample file not found: {INPUT_PATH}")

    df = pd.read_csv(
        INPUT_PATH,
        dtype={"sensor_id": "string"},
    )

    required_columns = {
        "sensor_id",
        "timestamp",
        "pressure_raw",
        "temperature",
        "humidity",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    results = []

    for record in df.to_dict(orient="records"):
        assessment = assess_measurement(
            pressure=record["pressure_raw"],
            temperature=record["temperature"],
            humidity=record["humidity"],
        )

        results.append(
            {
                "sensor_id": record["sensor_id"],
                "timestamp": record["timestamp"],
                "pressure_raw": record["pressure_raw"],
                "temperature_raw": record["temperature"],
                "humidity_raw": record["humidity"],
                **assessment,
            }
        )

    result_df = pd.DataFrame(results)

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_df.to_csv(OUTPUT_PATH, index=False)

    summary = (
        result_df.groupby(
            ["record_status", "pressure_status"],
            dropna=False,
        )
        .size()
        .reset_index(name="records")
        .sort_values(["record_status", "pressure_status"])
    )

    summary.to_csv(SUMMARY_PATH, index=False)

    print("\nMeasurement validation completed.")
    print(f"Records evaluated: {len(result_df):,}")
    print(f"Detailed report: {OUTPUT_PATH}")
    print(f"Summary report: {SUMMARY_PATH}")
    print("\nResults by status:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
