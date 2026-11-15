"""Send payment events from a pretend mobile app: mostly on time, some late, some broken.

Each event carries its own event_time (when the user paid on the phone). Phones that were
offline send their events late; a buggy app version sends broken records.
Run:  python producers/payment_events_producer.py            (300 events, about 1 minute)
      python producers/payment_events_producer.py --n 0     (until Ctrl+C)
"""
import argparse
import json
import random
import time
import uuid

from confluent_kafka import Producer

TOPIC = "payment-events"
USERS = [f"user-{n}" for n in range(1, 21)]
METHODS = ["card", "bkash", "nagad", "cash"]


def now_ms() -> int:
    return int(time.time() * 1000)


def good_event(delay_seconds: float = 0) -> dict:
    return {
        "payment_id": f"p-{uuid.uuid4().hex[:8]}",
        "user_id": random.choice(USERS),
        "amount": round(random.uniform(50, 3000), 2),
        "method": random.choice(METHODS),
        "event_time": now_ms() - int(delay_seconds * 1000),
    }


def broken_event() -> tuple[str, str]:
    """Return (kind, message text) for one of four realistic kinds of bad record."""
    event = good_event()
    kind = random.choice(["not_json", "missing_user", "comma_amount", "negative_amount"])
    if kind == "not_json":
        return kind, '{"payment_id": "p-broken", "amount": 120.5'      # cut off mid-message
    if kind == "missing_user":
        del event["user_id"]
    elif kind == "comma_amount":
        event["amount"] = f"{random.uniform(1000, 9000):,.2f}"           # "1,250.50": fixable later
    else:
        event["amount"] = -abs(event["amount"])
    return kind, json.dumps(event)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=300, help="number of events (0 = never stop)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})
    counts = {"on_time": 0, "slightly_delayed": 0, "very_late": 0, "broken": 0}
    sent = 0
    try:
        while args.n == 0 or sent < args.n:
            roll = random.random()
            if roll < 0.80:
                kind, value = "on_time", json.dumps(good_event())
            elif roll < 0.88:
                kind, value = "slightly_delayed", json.dumps(good_event(random.uniform(3, 8)))
            elif roll < 0.94:
                kind, value = "very_late", json.dumps(good_event(random.uniform(70, 180)))
            else:
                _, value = broken_event()
                kind = "broken"
            producer.produce(TOPIC, value=value)
            producer.poll(0)
            counts[kind] += 1
            sent += 1
            if sent % 50 == 0:
                print(f"sent {sent}: {counts}")
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"done: {sent} events {counts}")


if __name__ == "__main__":
    main()