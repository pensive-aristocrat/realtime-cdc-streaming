"""Print Debezium change events from the shop database as they arrive.

Run:  python consumers/cdc_watcher.py   (Ctrl+C to stop)
"""
import json

from confluent_kafka import Consumer

TOPICS = ["shop.public.orders", "shop.public.customers"]  # topic.prefix + schema + table
OPS = {"r": "SNAPSHOT", "c": "INSERT", "u": "UPDATE", "d": "DELETE"}


def short(row: dict | None) -> str:
    if row is None:
        return "-"
    keep = ("order_id", "customer_id", "status", "amount", "city")
    return ", ".join(f"{k}={row[k]}" for k in keep if k in row)


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "cdc-watcher",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(TOPICS)
print("Watching database changes (Ctrl+C to stop)...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        if msg.value() is None:
            # A tombstone follows every delete: it tells Kafka it may drop older events for this key.
            print(f"{msg.topic():<22} TOMBSTONE key={msg.key().decode()}")
            continue
        event = json.loads(msg.value())
        op = OPS.get(event["op"], event["op"])
        lag_ms = event["ts_ms"] - event["source"]["ts_ms"]  # database commit -> Debezium
        print(f"{msg.topic():<22} {op:<8} before[{short(event['before'])}]  "
              f"after[{short(event['after'])}]  ({lag_ms} ms)")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()