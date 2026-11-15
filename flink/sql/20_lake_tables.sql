-- Module 20: stream the shop's order changes into Iceberg tables.
-- Opened with: ./scripts/flink_sql.sh 20_lake_tables.sql
-- Remember: never put a comment after a ';'.
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.exec.source.idle-timeout' = '10 s';

-- The Debezium change events, read as a stream (same as Module 18, with its own consumer group).
CREATE TABLE order_events (
    `before`   ROW<order_id INT, customer_id INT, status STRING, amount STRING>,
    `after`    ROW<order_id INT, customer_id INT, status STRING, amount STRING>,
    `op`       STRING,
    `source`   ROW<ts_ms BIGINT>,
    event_time AS TO_TIMESTAMP_LTZ(`source`.ts_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'shop.public.orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-lake',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- An Iceberg catalog that keeps tables as plain folders under /opt/warehouse (= warehouse/ in your repo).
CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///opt/warehouse',
    'property-version' = '1'
);

CREATE DATABASE IF NOT EXISTS lake.shop;

-- Every change to every order, kept forever: the streaming version of a Bronze table.
CREATE TABLE IF NOT EXISTS lake.shop.order_changes (
    order_id    INT,
    customer_id INT,
    status      STRING,
    amount      DECIMAL(10, 2),
    `op`        STRING,
    changed_at  TIMESTAMP_LTZ(3),
    ingested_at TIMESTAMP_LTZ(3)
) WITH ('format-version' = '2');

-- New orders and revenue per minute: a Gold-style result table.
CREATE TABLE IF NOT EXISTS lake.shop.revenue_per_minute (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    new_orders   BIGINT,
    revenue      DECIMAL(12, 2)
) WITH ('format-version' = '2');