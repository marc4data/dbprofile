"""
Seed script — generates inputs/dev.duckdb with two NYC taxi-like tables.

Intentional data quality issues are embedded so all checks have something to find:
  - null_density:           passenger_count ~12% null; tip_amount ~8% null
  - uniqueness:             vendor_id is low-cardinality; pickup_datetime near-unique
  - numeric_distribution:   trip_distance has injected outliers (0.0 and 300+ mi)
  - frequency_distribution: vendor_id (2 values), payment_type (5 values), rate_code (6 values)
  - temporal_consistency:   ~10 day gap injected mid-year in yellow table
  - format_validation:      store_and_fwd_flag has a few bad values ('X', 'maybe')

Run from the project root:
    python scripts/seed_dev.py
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_PATH = Path(__file__).parent.parent / "inputs" / "dev.duckdb"
ROWS_YELLOW = 5_000
ROWS_GREEN  = 3_000
SEED        = 42

random.seed(SEED)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_dt(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def maybe_null(value, pct: float):
    """Return None with probability pct (0–1), otherwise return value."""
    return None if random.random() < pct else value


def generate_yellow(n: int) -> list[dict]:
    start = datetime(2022, 1, 1)
    # Gap: skip Jan 20 – Jan 29 to trigger temporal_consistency check
    gap_start = datetime(2022, 1, 20)
    gap_end   = datetime(2022, 1, 30)
    end       = datetime(2022, 3, 31)

    vendors      = ["1", "2"]
    rate_codes   = [1, 2, 3, 4, 5, 99]
    payment_types = ["Credit card", "Cash", "No charge", "Dispute", "Unknown"]
    flag_values  = ["Y", "N"]
    bad_flags    = ["X", "maybe", ""]   # injected format violations

    rows = []
    for i in range(n):
        # Avoid the gap window
        pickup = rand_dt(start, end)
        while gap_start <= pickup <= gap_end:
            pickup = rand_dt(start, end)

        duration   = timedelta(minutes=random.randint(3, 60))
        dropoff    = pickup + duration

        # Inject outlier trip distances for ~2% of rows
        if random.random() < 0.02:
            dist = round(random.choice([0.0, 0.0, 350.0, 420.0, 999.0]), 2)
        else:
            dist = round(random.uniform(0.5, 25.0), 2)

        fare = round(max(2.50, dist * 2.5 + random.uniform(-1, 3)), 2)
        tip  = round(random.uniform(0, fare * 0.3), 2)

        # Inject a few bad store_and_fwd_flag values
        if random.random() < 0.03:
            flag = random.choice(bad_flags)
        else:
            flag = random.choice(flag_values)

        rows.append({
            "vendor_id":         random.choice(vendors),
            "pickup_datetime":   pickup,
            "dropoff_datetime":  dropoff,
            "passenger_count":   maybe_null(random.randint(1, 6), 0.12),
            "trip_distance":     dist,
            "rate_code":         random.choice(rate_codes),
            "store_and_fwd_flag": flag,
            "payment_type":      random.choice(payment_types),
            "fare_amount":       fare,
            "extra":             round(random.choice([0.0, 0.5, 1.0]), 2),
            "mta_tax":           0.5,
            "tip_amount":        maybe_null(tip, 0.08),
            "tolls_amount":      round(random.choice([0.0, 0.0, 0.0, 6.12]), 2),
            "total_amount":      round(fare + tip + 0.5, 2),
        })
    return rows


def generate_green(n: int) -> list[dict]:
    start = datetime(2022, 1, 1)
    end   = datetime(2022, 3, 31)

    vendors      = ["2", "6"]
    payment_types = ["Credit card", "Cash", "No charge", "Dispute"]

    rows = []
    for i in range(n):
        pickup   = rand_dt(start, end)
        duration = timedelta(minutes=random.randint(3, 45))
        dropoff  = pickup + duration

        dist = round(random.uniform(0.5, 20.0), 2)
        fare = round(max(2.50, dist * 2.5 + random.uniform(-1, 3)), 2)
        tip  = round(random.uniform(0, fare * 0.25), 2)

        rows.append({
            "vendor_id":        random.choice(vendors),
            "pickup_datetime":  pickup,
            "dropoff_datetime": dropoff,
            "passenger_count":  maybe_null(random.randint(1, 5), 0.10),
            "trip_distance":    dist,
            "payment_type":     random.choice(payment_types),
            "fare_amount":      fare,
            "extra":            round(random.choice([0.0, 0.5, 1.0]), 2),
            "mta_tax":          0.5,
            "tip_amount":       maybe_null(tip, 0.06),
            "tolls_amount":     round(random.choice([0.0, 0.0, 6.12]), 2),
            "total_amount":     round(fare + tip + 0.5, 2),
        })
    return rows


# ---------------------------------------------------------------------------
# Write to DuckDB
# ---------------------------------------------------------------------------

def write_table(conn, schema: str, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())

    # Infer DDL types from first row
    type_map = {
        int:   "INTEGER",
        float: "DOUBLE",
        str:   "VARCHAR",
        datetime: "TIMESTAMP",
        type(None): "VARCHAR",
    }

    def col_type(key: str) -> str:
        for row in rows:
            v = row[key]
            if v is not None:
                return type_map.get(type(v), "VARCHAR")
        return "VARCHAR"

    ddl_cols = ", ".join(f'"{c}" {col_type(c)}' for c in cols)
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    conn.execute(f'CREATE TABLE "{schema}"."{table}" ({ddl_cols})')

    placeholders = ", ".join("?" * len(cols))
    data = [[row[c] for c in cols] for row in rows]
    conn.executemany(
        f'INSERT INTO "{schema}"."{table}" VALUES ({placeholders})',
        data,
    )
    print(f"  {schema}.{table}: {len(rows):,} rows written")


def main() -> None:
    import duckdb

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    conn = duckdb.connect(str(OUTPUT_PATH))

    print(f"Seeding {OUTPUT_PATH} ...")
    write_table(conn, "main", "tlc_yellow_trips_2022", generate_yellow(ROWS_YELLOW))
    write_table(conn, "main", "tlc_green_trips_2022",  generate_green(ROWS_GREEN))

    conn.close()
    print(f"Done. File size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
