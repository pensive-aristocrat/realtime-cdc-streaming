"""Query the Iceberg tables that Flink writes, straight from the warehouse folder, with DuckDB.

No server needed: DuckDB reads Iceberg's metadata and Parquet files directly.
Run:  python consumers/lake_check.py
"""
from pathlib import Path

import duckdb

WAREHOUSE = Path.home() / "streaming-warehouse" / "shop"

con = duckdb.connect()
con.sql("INSTALL iceberg; LOAD iceberg;")


def table(name: str) -> str:
    # allow_moved_paths: Flink wrote the paths as /opt/warehouse/... (inside Docker); read them from here.
    return f"iceberg_scan('{WAREHOUSE / name}', allow_moved_paths = true)"


print("Snapshots of order_changes (one per Flink checkpoint that had new data):")
con.sql(f"""
    SELECT count(*) AS snapshots, max(timestamp_ms) AS latest_commit
    FROM iceberg_snapshots('{WAREHOUSE / 'order_changes'}')
""").show()

print("Change events by type:")
con.sql(f"SELECT op, count(*) AS events FROM {table('order_changes')} GROUP BY op ORDER BY op").show()

print("Current orders, rebuilt from the change history (latest change per order, deletes removed):")
con.sql(f"""
    WITH latest AS (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY changed_at DESC, ingested_at DESC) AS rn
        FROM {table('order_changes')}
    )
    SELECT status, count(*) AS orders, sum(amount) AS revenue
    FROM latest
    WHERE rn = 1 AND op <> 'd'
    GROUP BY status ORDER BY orders DESC
""").show()

print("Revenue per minute (latest 5):")
con.sql(f"""
    SELECT window_start, new_orders, revenue
    FROM {table('revenue_per_minute')}
    ORDER BY window_start DESC LIMIT 5
""").show()