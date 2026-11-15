"""A live dashboard over the Iceberg tables, refreshed every 10 seconds.

Run:  streamlit run app/lake_dashboard.py      then open http://localhost:8501
"""
import time
from pathlib import Path

import duckdb
import streamlit as st

WAREHOUSE = Path.home() / "streaming-warehouse" / "shop"
REFRESH_SECONDS = 10


def table(name: str) -> str:
    return f"iceberg_scan('{WAREHOUSE / name}', allow_moved_paths = true)"


@st.cache_resource
def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    return con


st.set_page_config(page_title="Shop: live orders", layout="wide")
st.title("Shop orders, live from the lakehouse")
st.caption("PostgreSQL → Debezium → Kafka → Flink → Iceberg → DuckDB. Refreshes every "
           f"{REFRESH_SECONDS} seconds.")

con = connect().cursor()  # a fresh cursor per refresh; execute(...) works in every DuckDB version
current = con.execute(f"""
    WITH latest AS (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY changed_at DESC, ingested_at DESC) AS rn
        FROM {table('order_changes')}
    )
    SELECT status, count(*) AS orders, sum(amount) AS revenue
    FROM latest WHERE rn = 1 AND op <> 'd'
    GROUP BY status ORDER BY orders DESC
""").fetchdf()
per_minute = con.execute(f"""
    SELECT window_start, new_orders, revenue
    FROM {table('revenue_per_minute')}
    ORDER BY window_start DESC LIMIT 30
""").fetchdf()
freshness = con.execute(f"""
    SELECT count(*) AS changes,
           max(ingested_at) AS last_write,
           date_diff('second', max(changed_at), now()) AS seconds_behind
    FROM {table('order_changes')}
""").fetchone()

col1, col2, col3 = st.columns(3)
col1.metric("Open orders", int(current.loc[current["status"] != "delivered", "orders"].sum()))
col2.metric("Change events stored", f"{freshness[0]:,}")
col3.metric("Seconds behind the database", freshness[2] if freshness[2] is not None else "-")

left, right = st.columns(2)
left.subheader("Orders by status (now)")
left.bar_chart(current, x="status", y="orders")
right.subheader("Revenue from new orders, per minute")
right.line_chart(per_minute.sort_values("window_start"), x="window_start", y="revenue")

st.caption(f"Last change written to Iceberg: {freshness[1]}")
time.sleep(REFRESH_SECONDS)
st.rerun()