"""
Deliberately builds a BAD event (wrong data type for `fare`) and tries to
encode it with the Avro schema, to prove enforcement actually happens
before anything reaches Kafka.

Run it with:
    python3 layer2_schema_contracts/send_bad_event_demo.py
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from layer2_schema_contracts.avro_utils import get_latest_schema, encode_avro_message

SUBJECT_NAME = "rides_avro-value"


def main():
    schema_id, schema = get_latest_schema(SUBJECT_NAME)

    # A perfectly normal, valid event:
    good_event = {
        "ride_id": "abc-123",
        "rider_name": "Test Rider",
        "pickup_city": "Columbus",
        "dropoff_city": "Cleveland",
        "fare": 42.50,
        "status": "completed",
        "event_time": "2026-09-23T12:00:00+00:00",
    }

    # A BAD event: fare is a string instead of a number, and a required
    # field (status) is missing entirely.
    bad_event = {
        "ride_id": "def-456",
        "rider_name": "Test Rider 2",
        "pickup_city": "Columbus",
        "dropoff_city": "Cleveland",
        "fare": "forty two dollars",  # wrong type!
        "event_time": "2026-09-23T12:05:00+00:00",
        # "status" is missing entirely!
    }

    print("Trying to encode a GOOD event...")
    try:
        encoded = encode_avro_message(schema_id, schema, good_event)
        print(f"✅ Success -- encoded {len(encoded)} bytes.\n")
    except Exception as e:
        print(f"❌ Unexpected failure: {e}\n")

    print("Trying to encode a BAD event (wrong type + missing field)...")
    try:
        encoded = encode_avro_message(schema_id, schema, bad_event)
        print(f"⚠️  This should NOT have succeeded, but it did: {len(encoded)} bytes.")
    except Exception as e:
        print(f"✅ Correctly rejected before ever reaching Kafka: {e}")


if __name__ == "__main__":
    main()
