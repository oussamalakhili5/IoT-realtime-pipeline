
from pathlib import Path

import pandas as pd


INPUT_PATH = Path("reports/pressure_normalization_audit_by_sensor.csv")
OUTPUT_PATH = Path("reports/pressure_unit_diagnosis_by_sensor.csv")
SUMMARY_PATH = Path("reports/pressure_unit_diagnosis_summary.csv")

STATUSES = [
    "missing_or_nonnumeric",
    "invalid_nonpositive",
    "converted_from_hpa",
    "converted_from_hpa_out_of_range",
    "valid_pa",
    "outside_candidate_ranges",
]


def diagnose_unit(row: pd.Series) -> str:
    """Assign a provisional unit diagnosis based on observed value ranges."""
    hpa_count = (
        row["converted_from_hpa"]
        + row["converted_from_hpa_out_of_range"]
    )
    pa_count = row["valid_pa"]

    if hpa_count > 0 and pa_count > 0:
        return "mixed_scale_candidates"

    if hpa_count > 0:
        return "hpa_scale_candidate"

    if pa_count > 0:
        return "pa_scale_candidate"

    return "unresolved_scale"


def main() -> None:
    """Summarize pressure scales and quality categories for every sensor."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input report not found: {INPUT_PATH}")

    audit = pd.read_csv(
        INPUT_PATH,
        dtype={"sensor_id": "string"},
    )

    required = {"sensor_id", "quality_status", "records"}
    missing = required - set(audit.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    pivot = audit.pivot_table(
        index="sensor_id",
        columns="quality_status",
        values="records",
        aggfunc="sum",
        fill_value=0,
    )

    for status in STATUSES:
        if status not in pivot.columns:
            pivot[status] = 0

    pivot = pivot[STATUSES].astype("int64")
    pivot["total_records"] = pivot[STATUSES].sum(axis=1)

    pivot["hpa_candidate_records"] = (
        pivot["converted_from_hpa"]
        + pivot["converted_from_hpa_out_of_range"]
    )
    pivot["pa_candidate_records"] = pivot["valid_pa"]

    candidate_total = (
        pivot["hpa_candidate_records"]
        + pivot["pa_candidate_records"]
    )

    denominator = candidate_total.replace(0, float("nan"))

    pivot["hpa_candidate_percentage"] = (
        100 * pivot["hpa_candidate_records"] / denominator
    ).round(2)

    pivot["pa_candidate_percentage"] = (
        100 * pivot["pa_candidate_records"] / denominator
    ).round(2)

    pivot["unit_diagnosis"] = pivot.apply(
        diagnose_unit,
        axis=1,
    )

    result = pivot.reset_index().sort_values(
        ["unit_diagnosis", "sensor_id"]
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    summary = (
        result.groupby("unit_diagnosis")
        .agg(
            sensors=("sensor_id", "nunique"),
            records=("total_records", "sum"),
        )
        .reset_index()
        .sort_values("unit_diagnosis")
    )

    summary.to_csv(SUMMARY_PATH, index=False)

    print("\nPressure unit diagnosis completed.")
    print(f"Sensor report: {OUTPUT_PATH}")
    print(f"Summary report: {SUMMARY_PATH}")
    print("\nResults by diagnosis:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
