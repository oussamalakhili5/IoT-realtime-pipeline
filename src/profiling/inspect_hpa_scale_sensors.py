
from pathlib import Path

import pandas as pd


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")

DIAGNOSIS_PATH = Path(
    "reports/pressure_unit_diagnosis_by_sensor.csv"
)

OUTPUT_PATH = Path(
    "reports/hpa_scale_sensor_context.csv"
)

CHUNK_SIZE = 500_000

TEMPERATURE_MIN = -40
TEMPERATURE_MAX = 85
HUMIDITY_MIN = 0
HUMIDITY_MAX = 100


def main() -> None:
    """Analyze measurement context for sensors with hPa-like values."""
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

    target_ids = set(
        diagnosis.loc[
            diagnosis["unit_diagnosis"] == "hpa_scale_candidate",
            "sensor_id",
        ].dropna()
    )

    if not target_ids:
        raise ValueError("No hPa-scale candidate sensors found.")

    states = {}

    def create_state(sensor_id: str) -> dict:
        """Create an accumulator for one sensor."""
        return {
            "sensor_id": sensor_id,
            "total_records": 0,
            "pressure_numeric_records": 0,
            "pressure_missing_or_nonnumeric": 0,
            "pressure_nonpositive": 0,
            "pressure_hpa_candidate_records": 0,
            "pressure_pa_candidate_records": 0,
            "pressure_outside_candidate_ranges": 0,
            "hpa_pressure_min": None,
            "hpa_pressure_max": None,
            "temperature_missing_or_nonnumeric": 0,
            "temperature_out_of_range": 0,
            "temperature_min": None,
            "temperature_max": None,
            "humidity_missing_or_nonnumeric": 0,
            "humidity_out_of_range": 0,
            "humidity_zero_records": 0,
            "humidity_100_records": 0,
            "humidity_min": None,
            "humidity_max": None,
        }

    columns = [
        "sensor_id",
        "pressure",
        "temperature",
        "humidity",
    ]

    reader = pd.read_csv(
        DATA_PATH,
        sep=";",
        usecols=columns,
        dtype={"sensor_id": "string"},
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    total_rows_scanned = 0

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows_scanned += len(chunk)

        relevant = chunk[
            chunk["sensor_id"].isin(target_ids)
        ]

        if relevant.empty:
            continue

        relevant = relevant.copy()

        relevant["pressure_numeric"] = pd.to_numeric(
            relevant["pressure"], errors="coerce"
        )
        relevant["temperature_numeric"] = pd.to_numeric(
            relevant["temperature"], errors="coerce"
        )
        relevant["humidity_numeric"] = pd.to_numeric(
            relevant["humidity"], errors="coerce"
        )

        pressure = relevant["pressure_numeric"]
        temperature = relevant["temperature_numeric"]
        humidity = relevant["humidity_numeric"]

        relevant["pressure_hpa_candidate"] = pressure.between(
            300, 1200
        )

        relevant["pressure_pa_candidate"] = pressure.between(
            30_000, 110_000
        )

        for sensor_id, group in relevant.groupby("sensor_id"):
            sensor_id = str(sensor_id)

            stats = states.setdefault(
                sensor_id,
                create_state(sensor_id),
            )

            group_pressure = group["pressure_numeric"]
            group_temperature = group["temperature_numeric"]
            group_humidity = group["humidity_numeric"]

            hpa_rows = group.loc[
                group["pressure_hpa_candidate"]
            ]

            hpa_temperature = hpa_rows["temperature_numeric"]
            hpa_humidity = hpa_rows["humidity_numeric"]
            hpa_pressure = hpa_rows["pressure_numeric"]

            stats["total_records"] += len(group)

            stats["pressure_numeric_records"] += int(
                group_pressure.notna().sum()
            )

            stats["pressure_missing_or_nonnumeric"] += int(
                group_pressure.isna().sum()
            )

            stats["pressure_nonpositive"] += int(
                (group_pressure <= 0).sum()
            )

            stats["pressure_hpa_candidate_records"] += len(hpa_rows)

            stats["pressure_pa_candidate_records"] += int(
                group["pressure_pa_candidate"].sum()
            )

            stats["pressure_outside_candidate_ranges"] += int(
                (
                    group_pressure.notna()
                    & (group_pressure > 0)
                    & ~group["pressure_hpa_candidate"]
                    & ~group["pressure_pa_candidate"]
                ).sum()
            )

            if not hpa_pressure.empty:
                current_min = float(hpa_pressure.min())
                current_max = float(hpa_pressure.max())

                if stats["hpa_pressure_min"] is None:
                    stats["hpa_pressure_min"] = current_min
                    stats["hpa_pressure_max"] = current_max
                else:
                    stats["hpa_pressure_min"] = min(
                        stats["hpa_pressure_min"], current_min
                    )
                    stats["hpa_pressure_max"] = max(
                        stats["hpa_pressure_max"], current_max
                    )

            stats["temperature_missing_or_nonnumeric"] += int(
                hpa_temperature.isna().sum()
            )

            stats["temperature_out_of_range"] += int(
                (
                    hpa_temperature.notna()
                    & ~hpa_temperature.between(
                        TEMPERATURE_MIN,
                        TEMPERATURE_MAX,
                    )
                ).sum()
            )

            valid_temperature = hpa_temperature.dropna()

            if not valid_temperature.empty:
                current_min = float(valid_temperature.min())
                current_max = float(valid_temperature.max())

                if stats["temperature_min"] is None:
                    stats["temperature_min"] = current_min
                    stats["temperature_max"] = current_max
                else:
                    stats["temperature_min"] = min(
                        stats["temperature_min"], current_min
                    )
                    stats["temperature_max"] = max(
                        stats["temperature_max"], current_max
                    )

            stats["humidity_missing_or_nonnumeric"] += int(
                hpa_humidity.isna().sum()
            )

            stats["humidity_out_of_range"] += int(
                (
                    hpa_humidity.notna()
                    & ~hpa_humidity.between(
                        HUMIDITY_MIN,
                        HUMIDITY_MAX,
                    )
                ).sum()
            )

            stats["humidity_zero_records"] += int(
                (hpa_humidity == 0).sum()
            )

            stats["humidity_100_records"] += int(
                (hpa_humidity == 100).sum()
            )

            valid_humidity = hpa_humidity.dropna()

            if not valid_humidity.empty:
                current_min = float(valid_humidity.min())
                current_max = float(valid_humidity.max())

                if stats["humidity_min"] is None:
                    stats["humidity_min"] = current_min
                    stats["humidity_max"] = current_max
                else:
                    stats["humidity_min"] = min(
                        stats["humidity_min"], current_min
                    )
                    stats["humidity_max"] = max(
                        stats["humidity_max"], current_max
                    )

        if chunk_number % 25 == 0:
            print(
                f"Processed {chunk_number} chunks "
                f"({total_rows_scanned:,} rows)..."
            )

    result = pd.DataFrame(states.values())

    result["temperature_out_of_range_pct"] = (
        100 * result["temperature_out_of_range"]
        / result["pressure_hpa_candidate_records"].replace(0, pd.NA)
    )

    result["humidity_boundary_pct"] = (
        100
        * (
            result["humidity_zero_records"]
            + result["humidity_100_records"]
        )
        / result["pressure_hpa_candidate_records"].replace(0, pd.NA)
    )

    result = result.sort_values(
        [
            "temperature_out_of_range_pct",
            "humidity_boundary_pct",
        ],
        ascending=False,
        na_position="last",
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print("\nHpa-scale sensor context analysis completed.")
    print(f"Target sensors: {len(target_ids)}")
    print(
        "Pressure candidate records: "
        f"{int(result['pressure_hpa_candidate_records'].sum()):,}"
    )
    print(f"Report: {OUTPUT_PATH}")

    print("\nTop 20 sensors by temperature anomalies:")
    print(
        result[
            [
                "sensor_id",
                "pressure_hpa_candidate_records",
                "hpa_pressure_min",
                "hpa_pressure_max",
                "temperature_out_of_range",
                "temperature_out_of_range_pct",
                "humidity_zero_records",
                "humidity_100_records",
                "humidity_boundary_pct",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()
