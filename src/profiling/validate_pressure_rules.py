
from pathlib import Path

import pandas as pd

from src.streaming.pressure_rules import normalize_pressure


SAMPLE_PATH = Path("reports/pressure_record_samples.csv")
OUTPUT_PATH = Path("reports/pressure_rules_validation.csv")
SUMMARY_PATH = Path("reports/pressure_rules_validation_summary.csv")


EXPECTED_STATUSES = {
    "missing_or_nonnumeric": {"missing_or_nonnumeric"},
    "zero_or_negative": {"invalid_nonpositive"},
    "hPa_like": {
        "converted_from_hpa",
        "converted_from_hpa_out_of_range",
    },
    "Pa_like": {"valid_pa"},
    "outside_candidate_ranges": {"outside_candidate_ranges"},
}


def main() -> None:
    """Validate pressure normalization rules against real sample records."""
    if not SAMPLE_PATH.exists():
        raise FileNotFoundError(
            f"Sample file not found: {SAMPLE_PATH}"
        )

    samples = pd.read_csv(
        SAMPLE_PATH,
        dtype={"sensor_id": "string", "pressure_raw": "string"},
    )

    if samples.empty:
        raise ValueError("The pressure sample file is empty.")

    required_columns = {
        "sensor_id",
        "pressure_raw",
        "pressure_category",
    }

    missing_columns = required_columns - set(samples.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    normalized_records = []

    for record in samples.to_dict(orient="records"):
        pressure_pa, inferred_unit, quality_status = normalize_pressure(
            record["pressure_raw"]
        )

        expected_statuses = EXPECTED_STATUSES.get(
            record["pressure_category"], set()
        )

        record["pressure_pa_candidate"] = pressure_pa
        record["inferred_unit"] = inferred_unit
        record["quality_status"] = quality_status
        record["status_matches_candidate_category"] = (
            quality_status in expected_statuses
        )

        normalized_records.append(record)

    results = pd.DataFrame(normalized_records)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    results.to_csv(OUTPUT_PATH, index=False)

    summary = (
        results.groupby(
            ["sensor_id", "quality_status"],
            dropna=False,
        )
        .size()
        .reset_index(name="records")
        .sort_values(["sensor_id", "quality_status"])
    )

    summary.to_csv(SUMMARY_PATH, index=False)

    consistent = int(
        results["status_matches_candidate_category"].sum()
    )
    total = len(results)

    print("\nPressure rules validation completed.")
    print(f"Sample records evaluated: {total}")
    print(f"Category-consistent records: {consistent}/{total}")
    print(
        "Category mismatches:",
        total - consistent,
    )
    print(f"\nDetailed results: {OUTPUT_PATH}")
    print(f"Summary: {SUMMARY_PATH}")
    print("\nSummary by sensor and status:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
