"""Ask Schema Registry whether two proposed changes to the schema are safe (BACKWARD compatible).

Run:  python producers/evolution_demo.py
"""
import copy
import json
from pathlib import Path

import requests

REGISTRY = "http://localhost:8081"
SUBJECT = "wiki-edits-value"  # topic name + "-value": the default naming rule
current = json.loads((Path(__file__).resolve().parent.parent / "schemas" / "wiki_edit.avsc").read_text())

# Change 1: add an optional field with a default. Old data simply gets the default.
add_field = copy.deepcopy(current)
add_field["fields"].append({"name": "minor", "type": "boolean", "default": False})

# Change 2: change a field's type. Old data (int) can no longer be read as the new type (string).
change_type = copy.deepcopy(current)
for field in change_type["fields"]:
    if field["name"] == "namespace":
        field["type"] = "string"

print("compatibility rule:", requests.get(f"{REGISTRY}/config").json())
for name, schema in [("add 'minor' with default", add_field), ("namespace int -> string", change_type)]:
    response = requests.post(
        f"{REGISTRY}/compatibility/subjects/{SUBJECT}/versions/latest",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": json.dumps(schema)},
    )
    print(f"{name:<28} -> compatible: {response.json().get('is_compatible')}")