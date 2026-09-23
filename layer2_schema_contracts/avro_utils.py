"""
Shared helpers for encoding/decoding messages using Confluent's Avro wire format.

Confluent's format for a Kafka message value looks like this, byte by byte:
  [1 byte: magic byte, always 0]
  [4 bytes: the schema ID from Schema Registry, big-endian integer]
  [N bytes: the actual Avro-encoded data]

Why the magic byte + schema ID? So that ANY consumer, even one that's never
seen this exact schema before, can look up the schema ID in Schema Registry
and learn exactly how to decode the bytes that follow. This is what lets
producers and consumers evolve independently over time.
"""

import io
import struct
import requests
from fastavro import schemaless_writer, schemaless_reader

SCHEMA_REGISTRY_URL = "http://localhost:8081"

MAGIC_BYTE = 0


def get_latest_schema(subject: str) -> tuple[int, dict]:
    """Fetches the current registered schema for a subject.
    Returns (schema_id, parsed_schema_dict)."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    import json
    return data["id"], json.loads(data["schema"])


def encode_avro_message(schema_id: int, schema: dict, event: dict) -> bytes:
    """Turns a Python dict into Confluent-wire-format Avro bytes.
    Raises an error if `event` doesn't match `schema` -- this is the
    enforcement we actually want."""
    buffer = io.BytesIO()
    buffer.write(struct.pack(">bI", MAGIC_BYTE, schema_id))
    schemaless_writer(buffer, schema, event)
    return buffer.getvalue()


def decode_avro_message(raw_bytes: bytes, schema_cache: dict) -> dict:
    """Reads Confluent-wire-format Avro bytes back into a Python dict.
    `schema_cache` is a dict we build up as {schema_id: schema_dict} so
    we don't hit Schema Registry over the network for every single message."""
    buffer = io.BytesIO(raw_bytes)
    magic, schema_id = struct.unpack(">bI", buffer.read(5))

    if schema_id not in schema_cache:
        url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
        response = requests.get(url)
        response.raise_for_status()
        import json
        schema_cache[schema_id] = json.loads(response.json()["schema"])

    schema = schema_cache[schema_id]
    return schemaless_reader(buffer, schema)
