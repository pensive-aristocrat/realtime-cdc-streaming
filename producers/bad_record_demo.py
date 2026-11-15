"""Show Schema Registry + Avro stopping bad data before it reaches Kafka.

Run:  python producers/bad_record_demo.py
"""
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

SCHEMA = (Path(__file__).resolve().parent.parent / "schemas" / "wiki_edit.avsc").read_text()
serializer = AvroSerializer(SchemaRegistryClient({"url": "http://localhost:8081"}), SCHEMA)
ctx = SerializationContext("wiki-edits", MessageField.VALUE)

good = {
    "event_id": 1, "wiki": "enwiki", "server_name": "en.wikipedia.org", "title": "Kafka",
    "user": "demo", "bot": False, "change_type": "edit", "namespace": 0,
    "length_old": 100, "length_new": 120, "event_time": datetime.now(timezone.utc),
}
bad_cases = {
    "title missing": {k: v for k, v in good.items() if k != "title"},
    "namespace is text": {**good, "namespace": "article"},
    "bot is None": {**good, "bot": None},
}

print(f"good record -> {len(serializer(good, ctx))} bytes of Avro (5-byte header + data)")
for name, record in bad_cases.items():
    try:
        serializer(record, ctx)
        print(f"{name:<18} -> accepted (unexpected!)")
    except Exception as err:  # the serializer refuses: nothing is sent
        print(f"{name:<18} -> REJECTED: {type(err).__name__}: {str(err)[:80]}")