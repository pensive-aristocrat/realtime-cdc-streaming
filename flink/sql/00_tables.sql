-- Note: never put a comment after a ';' here. The client only sees a statement end when a line ENDS with ';'.
-- Print results as a scrolling table:
SET 'sql-client.execution.result-mode' = 'tableau';
-- Quiet partitions must not hold back event time:
SET 'table.exec.source.idle-timeout' = '10 s';

-- 1) The orders table as a CHANGELOG: Flink applies each insert, update and delete.
CREATE TABLE orders (
    order_id    INT,
    customer_id INT,
    status      STRING,
    amount      STRING,
    updated_at  STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'shop.public.orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- 2) The same topic as a plain STREAM of change events, with event time and a watermark.
CREATE TABLE order_events (
    `before`   ROW<order_id INT, customer_id INT, status STRING, amount STRING>,
    `after`    ROW<order_id INT, customer_id INT, status STRING, amount STRING>,
    `op`       STRING,
    `source`   ROW<ts_ms BIGINT>,
    event_time AS TO_TIMESTAMP_LTZ(`source`.ts_ms, 3),          -- when the change was committed
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND -- wait 5 s for stragglers
) WITH (
    'connector' = 'kafka',
    'topic' = 'shop.public.orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-order-events',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- 3) Where the per-minute results are written: a new Kafka topic.
CREATE TABLE revenue_per_minute (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    new_orders   BIGINT,
    revenue      DECIMAL(12, 2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'revenue-per-minute',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);