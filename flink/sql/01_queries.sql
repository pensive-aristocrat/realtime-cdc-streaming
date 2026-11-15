SELECT status, COUNT(*) AS orders, SUM(CAST(amount AS DECIMAL(10, 2))) AS revenue
FROM orders
GROUP BY status;

SELECT window_start, window_end,
       COUNT(*) AS new_orders,
       SUM(CAST(`after`.amount AS DECIMAL(10, 2))) AS revenue
FROM TABLE(TUMBLE(TABLE order_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE `op` = 'c'
GROUP BY window_start, window_end;

SELECT window_start, window_end, COUNT(*) AS payments
FROM TABLE(HOP(TABLE order_events, DESCRIPTOR(event_time), INTERVAL '30' SECOND, INTERVAL '2' MINUTE))
WHERE `op` = 'u' AND `after`.status = 'paid'
GROUP BY window_start, window_end;

INSERT INTO revenue_per_minute
SELECT window_start, window_end,
       COUNT(*) AS new_orders,
       CAST(SUM(CAST(`after`.amount AS DECIMAL(10, 2))) AS DECIMAL(12, 2)) AS revenue
FROM TABLE(TUMBLE(TABLE order_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE `op` = 'c'
GROUP BY window_start, window_end;