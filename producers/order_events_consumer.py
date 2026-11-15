"""Read order events as part of a consumer group. Start two copies to see sharing.

Run:  python producers/order_events_consumer.py
"""
import json

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-dashboard",      # consumers with the same group share the partitions
    "auto.offset.reset": "earliest",    # a new group starts from the oldest message
})
consumer.subscribe(["order-events"])
print("Waiting for events (Ctrl+C to stop)...")

try:
    while True:
        msg = consumer.poll(1.0)        # wait up to 1 second for a message
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        event = json.loads(msg.value())
        print(f"partition {msg.partition()} offset {msg.offset():>3}  "
              f"{event['customer_id']:<12} {event['status']:<10} {event['amount']:>7}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()                    # commit progress and leave the group cleanly