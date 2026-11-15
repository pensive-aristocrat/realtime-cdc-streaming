"""Read the dead-letter queue, fix what can be fixed, and send it back for reprocessing.

Only one kind of error is safe to fix automatically here: amounts written with a thousands
separator ("1,250.50"). Everything else is counted and left for a human to look at.
Run:  python consumers/dlq_replay.py            (report only, changes nothing)
      python consumers/dlq_replay.py --replay   (also re-sends the fixable records)
"""
import argparse
import json
from collections import Counter

from confluent_kafka import Consumer, Producer

DLQ_TOPIC = "payments-dlq"
SOURCE_TOPIC = "payment-events"


def try_fix(payload: str) -> str | None:
    """Return a corrected message, or None if this record cannot be fixed safely."""
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return None
    amount = event.get("amount")
    if isinstance(amount, str) and "," in amount:
        event["amount"] = float(amount.replace(",", ""))
        return json.dumps(event)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", action="store_true", help="re-send fixable records")
    args = parser.parse_args()

    # A new group id each run, so the whole DLQ is read every time (this is a report).
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dlq-report-{int(__import__('time').time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([DLQ_TOPIC])
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

    reasons: Counter = Counter()
    fixable = replayed = 0
    empty_polls = 0
    while empty_polls < 5:                     # stop after 5 seconds with no new records
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        empty_polls = 0
        if msg.error() or msg.value() is None:
            continue
        record = json.loads(msg.value())
        reasons[record["reason"]] += 1
        fixed = try_fix(record["payload"])
        if fixed is not None:
            fixable += 1
            if args.replay:
                producer.produce(SOURCE_TOPIC, value=fixed)
                replayed += 1
    consumer.close()
    producer.flush()

    print(f"DLQ records: {sum(reasons.values())}")
    for reason, count in reasons.most_common():
        print(f"  {reason:<28} {count}")
    print(f"fixable automatically: {fixable}")
    print(f"re-sent to {SOURCE_TOPIC}: {replayed}" if args.replay else "run with --replay to re-send them")


if __name__ == "__main__":
    main()