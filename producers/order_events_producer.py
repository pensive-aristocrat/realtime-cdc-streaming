"""Send fake order events to Kafka, keyed by customer, to see partitions in action.

Run:  python producers/order_events_producer.py
"""
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "order-events"
CUSTOMERS = [f"customer-{n}" for n in range(1, 6)]
STATUSES = ["created", "paid", "shipped", "delivered"]


def delivery_report(err, msg) -> None:
    """Kafka calls this once per message: success shows where it landed."""
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"key={msg.key().decode():<12} -> partition {msg.partition()}  offset {msg.offset()}")


def main() -> None:
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})
    for i in range(20):
        customer = random.choice(CUSTOMERS)
        event = {
            "event_id": i,
            "customer_id": customer,
            "status": random.choice(STATUSES),
            "amount": round(random.uniform(10, 500), 2),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        # Same key -> same partition -> events for one customer stay in order.
        producer.produce(TOPIC, key=customer, value=json.dumps(event),
                         callback=delivery_report)
        producer.poll(0)  # let Kafka report deliveries as we go
        time.sleep(0.2)
    producer.flush()  # wait until every message is confirmed


if __name__ == "__main__":
    main()