"""Stream live Wikipedia edits into Kafka as Avro, with the schema kept in Schema Registry.

Run:  python producers/wiki_producer.py            (stops after 500 events)
      python producers/wiki_producer.py --max 0    (runs until Ctrl+C)
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext, SerializationError,
                                           StringSerializer)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia blocks requests without a descriptive User-Agent (name/version + a contact link).
USER_AGENT = "realtime-cdc-streaming/0.1 (https://github.com/pensive-aristocrat/realtime-cdc-streaming) python-requests"
TOPIC = "wiki-edits"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "wiki_edit.avsc"


def to_record(change: dict) -> dict:
    """Pick and rename the fields we keep. The Avro schema decides what is allowed."""
    length = change.get("length") or {}
    return {
        "event_id": change.get("id"),
        "wiki": change["wiki"],
        "server_name": change["server_name"],
        "title": change["title"],
        "user": change.get("user", "unknown"),
        "bot": bool(change.get("bot", False)),
        "change_type": change["type"],
        "namespace": int(change["namespace"]),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "event_time": datetime.fromtimestamp(change["timestamp"], tz=timezone.utc),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=500, help="stop after N events (0 = never)")
    args = parser.parse_args()

    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    value_serializer = AvroSerializer(registry, SCHEMA_PATH.read_text())  # registers the schema on first use
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all", "linger.ms": 50})

    sent = skipped = 0
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}  # one JSON object per line
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=30) as response:
        response.raise_for_status()
        try:
            for line in response.iter_lines():
                if not line:
                    continue
                change = json.loads(line)
                if change.get("meta", {}).get("domain") == "canary":  # Wikimedia's test events
                    continue
                try:
                    record = to_record(change)
                    value = value_serializer(record, SerializationContext(TOPIC, MessageField.VALUE))
                except (KeyError, ValueError, TypeError, SerializationError) as err:
                    skipped += 1  # does not fit the schema: rejected before it reaches Kafka
                    print(f"skipped: {err}")
                    continue
                producer.produce(TOPIC, key=key_serializer(record["wiki"]), value=value)
                producer.poll(0)
                sent += 1
                if sent % 50 == 0:
                    print(f"sent {sent} events (last: {record['wiki']} | {record['title'][:50]})")
                if args.max and sent >= args.max:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            producer.flush()
            print(f"done: {sent} sent, {skipped} skipped")


if __name__ == "__main__":
    main()