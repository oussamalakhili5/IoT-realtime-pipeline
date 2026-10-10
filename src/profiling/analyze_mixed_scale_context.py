
from pathlib import Path

import pandas as pd


INPUT_PATH = Path("reports/mixed_scale_pressure_candidates.csv")

SENSOR_REPORT_PATH = Path(
    "reports/mixed_scale_context_by_sensor.csv"
)
VALUE_REPORT_PATH = Path(
    "reports/mixed_scale_context_by_pressure_value.csv"
)

TEMPERATURE_MIN = -40
TEMPERATURE_MAX = 85
HUMIDITY_MIN = 0
HUMIDITY_MAX = 100


def main() -> None:
    """Analyze temperature and humidity associated with pressure candidates."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_csv(
        INPUT_PATH,
        dtype={"sensor_id": "string"},
    )

    required_columns = {
        "sensor_id",
        "pressure_raw",
        "quality_status",
        "temperature",
        "humidity",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    df["pressure_numeric"] = pd.to_numeric(
        df["pressure_raw"], errors="coerce"
    )
    df["temperature_numeric"] = pd.to_numeric(
        df["temperature"], errors="coerce"
    )
    df["humidity_numeric"] = pd.to_numeric(
        df["humidity"], errors="coerce"
    )

    temperature = df["temperature_numeric"]
    humidity = df["humidity_numeric"]

    df["temperature_missing_or_nonnumeric"] = temperature.isna()
    df["temperature_out_of_range"] = (
        temperature.notna()
        & ~temperature.between(
            TEMPERATURE_MIN, TEMPERATURE_MAX
        )
    )

    df["humidity_missing_or_nonnumeric"] = humidity.isna()
    df["humidity_out_of_range"] = (
        humidity.notna()
        & ~humidity.between(HUMIDITY_MIN, HUMIDITY_MAX)
    )

    sensor_records = []

    for sensor_id, group in df.groupby("sensor_id"):
        temp = group["temperature_numeric"].dropna()
        hum = group["humidity_numeric"].dropna()
        pressure = group["pressure_numeric"].dropna()

        sensor_records.append(
            {
                "sensor_id": sensor_id,
                "candidate_records": len(group),
                "distinct_pressure_values": pressure.nunique(),
                "pressure_min": pressure.min(),
                "pressure_max": pressure.max(),
                "temperature_missing_or_nonnumeric": int(
                    group["temperature_missing_or_nonnumeric"].sum()
                ),
                "temperature_out_of_range": int(
                    group["temperature_out_of_range"].sum()
                ),
                "temperature_min": temp.min(),
                "temperature_max": temp.max(),
                "humidity_missing_or_nonnumeric": int(
                    group["humidity_missing_or_nonnumeric"].sum()
                ),
                "humidity_out_of_range": int(
                    group["humidity_out_of_range"].sum()
                ),
                "humidity_min": hum.min(),
                "humidity_max": hum.max(),
                "converted_from_hpa_count": int(
                    (
                        group["quality_status"]
                        == "converted_from_hpa"
                    ).sum()
                ),
                "converted_from_hpa_out_of_range_count": int(
                    (
                        group["quality_status"]
                        == "converted_from_hpa_out_of_range"
                    ).sum()
                ),
            }
        )

    sensor_report = pd.DataFrame(sensor_records).sort_values(
        ["temperature_out_of_range", "candidate_records"],
        ascending=False,
    )

    value_report = (
        df.groupby(
            ["sensor_id", "pressure_raw", "quality_status"],
            dropna=False,
        )
        .agg(
            occurrences=("pressure_raw", "size"),
            temperature_min=("temperature_numeric", "min"),
            temperature_max=("temperature_numeric", "max"),
            temperature_out_of_range_count=(
                "temperature_out_of_range",
                "sum",
            ),
            humidity_min=("humidity_numeric", "min"),
            humidity_max=("humidity_numeric", "max"),
            humidity_out_of_range_count=(
                "humidity_out_of_range",
                "sum",
            ),
        )
        .reset_index()
        .sort_values(
            ["sensor_id", "occurrences"],
            ascending=[True, False],
        )
    )

    SENSOR_REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    sensor_report.to_csv(
        SENSOR_REPORT_PATH,
        index=False,
    )
    value_report.to_csv(
        VALUE_REPORT_PATH,
        index=False,
    )

    print("\nContext analysis completed.")
    print(f"Candidate records analyzed: {len(df):,}")
    print(f"Sensor report: {SENSOR_REPORT_PATH}")
    print(f"Value report: {VALUE_REPORT_PATH}")

    print("\nResults by sensor:")
    print(sensor_report.to_string(index=False))


if __name__ == "__main__":
    main()
