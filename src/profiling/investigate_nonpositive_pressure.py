
from collections import Counter
from pathlib import Path

import pandas as pd


DATA_PATH = Path("data/raw/2026-03_bme280/2026-03_bme280.csv")
OUTPUT_PATH = Path("reports/recurring_nonpositive_pressure_values.csv")

CHUNK_SIZE = 500_000
TOP_N = 20

TARGET_SENSOR_IDS = {"39287", "93517", "54381", "78677"}


def main() -> None:
    """Identify frequently repeated nonpositive pressure values."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    value_counts = {
        sensor_id: Counter()
        for sensor_id in TARGET_SENSOR_IDS
    }
    total_nonpositive = Counter()

    reader = pd.read_csv(
        DATA_PATH,
        sep=";",
        usecols=["sensor_id", "pressure"],
        dtype={"sensor_id": "string"},
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk[
            chunk["sensor_id"].isin(TARGET_SENSOR_IDS)
        ].copy()

        if chunk.empty:
            continue

        chunk["pressure_numeric"] = pd.to_numeric(
            chunk["pressure"],
            errors="coerce",
        )

        mask = (
            chunk["pressure_numeric"].notna()
            & (chunk["pressure_numeric"] <= 0)
        )

        invalid = chunk.loc[
            mask, ["sensor_id", "pressure_numeric"]
        ]

        for sensor_id, group in invalid.groupby("sensor_id"):
            sensor_id = str(sensor_id)

            counts = group["pressure_numeric"].value_counts()

            for pressure_value, count in counts.items():
                value_counts[sensor_id][float(pressure_value)] += int(
                    count
                )

            total_nonpositive[sensor_id] += len(group)

        if chunk_number % 50 == 0:
            print(f"Processed {chunk_number} chunks...")

    records = []

    for sensor_id in sorted(TARGET_SENSOR_IDS):
        total = total_nonpositive[sensor_id]

        for pressure_value, count in value_counts[
            sensor_id
        ].most_common(TOP_N):
            records.append(
                {
                    "sensor_id": sensor_id,
                    "pressure_value": pressure_value,
                    "occurrences": count,
                    "total_nonpositive": total,
                    "percentage_of_nonpositive": (
                        100 * count / total if total else 0
                    ),
                }
            )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(records).to_csv(OUTPUT_PATH, index=False)

    print("\nInvestigation completed.")
    print(f"Report: {OUTPUT_PATH}")

    for sensor_id in sorted(TARGET_SENSOR_IDS):
        print(
            f"Sensor {sensor_id}: "
            f"{total_nonpositive[sensor_id]:,} nonpositive records"
        )


if __name__ == "__main__":
    main()
