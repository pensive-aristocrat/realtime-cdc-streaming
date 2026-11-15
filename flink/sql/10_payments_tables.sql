-- Module 19: payment events with late data and a dead-letter queue.
-- Opened with: ./scripts/flink_sql.sh 10_payments_tables.sql
-- Remember: never put a comment after a ';'.
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.exec.source.idle-timeout' = '10 s';

-- Raw messages exactly as they arrived: one text column, no parsing yet, so nothing can fail here.
CREATE TABLE payments_raw (
    payload STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'payment-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-payments-validate',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

-- Each message checked: the parsed fields, plus the reason it is bad (NULL when it is fine).
CREATE TEMPORARY VIEW payments_checked AS
SELECT
    payload,
    JSON_VALUE(payload, '$.payment_id') AS payment_id,
    JSON_VALUE(payload, '$.user_id') AS user_id,
    TRY_CAST(JSON_VALUE(payload, '$.amount') AS DECIMAL(10, 2)) AS amount,
    JSON_VALUE(payload, '$.method') AS payment_method,
    TRY_CAST(JSON_VALUE(payload, '$.event_time') AS BIGINT) AS event_time_ms,
    CASE
        WHEN NOT (payload IS JSON) THEN 'not valid JSON'
        WHEN JSON_VALUE(payload, '$.payment_id') IS NULL THEN 'missing payment_id'
        WHEN JSON_VALUE(payload, '$.user_id') IS NULL THEN 'missing user_id'
        WHEN TRY_CAST(JSON_VALUE(payload, '$.amount') AS DECIMAL(10, 2)) IS NULL THEN 'amount is not a number'
        WHEN TRY_CAST(JSON_VALUE(payload, '$.amount') AS DECIMAL(10, 2)) <= 0 THEN 'amount must be positive'
        WHEN TRY_CAST(JSON_VALUE(payload, '$.event_time') AS BIGINT) IS NULL THEN 'missing event_time'
        ELSE CAST(NULL AS STRING)
    END AS reason
FROM payments_raw;

-- Good records, written to a clean topic and read back with event time and a watermark.
CREATE TABLE payments_clean (
    payment_id    STRING,
    user_id       STRING,
    amount        DECIMAL(10, 2),
    payment_method        STRING,
    event_time_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments-clean',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-payments-agg',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Bad records: the original message, why it failed, and when.
CREATE TABLE payments_dlq (
    payload   STRING,
    reason    STRING,
    failed_at TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments-dlq',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- Results: payments per minute and method.
CREATE TABLE payments_per_minute (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    payment_method       STRING,
    payments     BIGINT,
    total        DECIMAL(12, 2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments-per-minute',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- Late records: arrived after the watermark had passed their event time.
CREATE TABLE payments_late (
    payment_id           STRING,
    user_id              STRING,
    amount               DECIMAL(10, 2),
    event_time           TIMESTAMP_LTZ(3),
    watermark_at_arrival TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments-late',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);