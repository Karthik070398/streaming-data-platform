"""
Talks to Schema Registry's REST API to:
  1. check if a NEW schema is compatible with what's already registered
  2. register a schema for real, if desired

Schema Registry organizes schemas under a "subject" name. By convention,
for a topic named "rides" where the value (the message body) is Avro,
the subject is "rides-value".

Run examples:
    python3 layer2_schema_contracts/schema_tool.py check rides-value schemas/ride_event_v2_safe.avsc
    python3 layer2_schema_contracts/schema_tool.py register rides-value schemas/ride_event_v1.avsc
"""

import sys
import json
import requests

SCHEMA_REGISTRY_URL = "http://localhost:8081"


def load_schema_string(path: str) -> str:
    with open(path, "r") as f:
        # Schema Registry wants the schema as a JSON *string* inside another
        # JSON object, so we load it then re-serialize it to a compact string.
        schema_dict = json.load(f)
        return json.dumps(schema_dict)


def check_compatibility(subject: str, schema_path: str):
    """Ask the librarian: 'if I used this new form, would it still make
    sense to people using the old form?' -- WITHOUT actually registering it."""
    schema_str = load_schema_string(schema_path)
    url = f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
    response = requests.post(
        url,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": schema_str}),
    )

    if response.status_code == 404:
        print(f"No existing schema found for subject '{subject}' yet.")
        print("Nothing to check compatibility against -- register a v1 first.")
        return

    result = response.json()
    if result.get("is_compatible"):
        print(f"✅ COMPATIBLE — '{schema_path}' can safely replace the current schema.")
    else:
        print(f"❌ REJECTED — '{schema_path}' is NOT compatible with the current schema.")
        print("   Schema Registry would refuse to register this change.")


def register_schema(subject: str, schema_path: str):
    """Actually register a schema as the new official version for a subject."""
    schema_str = load_schema_string(schema_path)
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    response = requests.post(
        url,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": schema_str}),
    )

    if response.status_code == 200:
        schema_id = response.json()["id"]
        print(f"✅ Registered '{schema_path}' under subject '{subject}' (schema id: {schema_id})")
    else:
        print(f"❌ Registration failed ({response.status_code}):")
        print(response.text)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 schema_tool.py [check|register] <subject> <path_to_schema.avsc>")
        sys.exit(1)

    action, subject, path = sys.argv[1], sys.argv[2], sys.argv[3]

    if action == "check":
        check_compatibility(subject, path)
    elif action == "register":
        register_schema(subject, path)
    else:
        print(f"Unknown action '{action}'. Use 'check' or 'register'.")
