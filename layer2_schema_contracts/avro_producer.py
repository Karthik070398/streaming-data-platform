"""
Sends ride events to Kafka, but this time every message is strictly
encoded according to the registered Avro schema BEFORE it's allowed
onto the wire. A malformed event never reaches Kafka at all -- it fails
right here, in your own code, with a clear error.

Run it with:
    python3 layer2_schema_contracts/avro_producer.py
"""

import sys
import os
import time

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.event_generator import generate_ride_event
from layer2_schema_contracts.avro_utils import get_latest_schema, encode_avro_message

TOPIC_NAME = "rides_avro"
SUBJECT_NAME = "rides_avro-value"
KAFKA_BROKER = "localhost:9092"


def main():
    # Fetch the schema ONCE at startup -- we assume it was already
    # registered (we'll do that as part of running this script's setup).
    schema_id, schema = get_latest_schema(SUBJECT_NAME)
    print(f"Using schema id {schema_id} for subject '{SUBJECT_NAME}'")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # No JSON serializer this time -- we're handing Kafka raw,
        # already-Avro-encoded bytes ourselves.
        value_serializer=lambda v: v,
    )

    print(f"Sending Avro-encoded events to '{TOPIC_NAME}' (Ctrl+C to stop)...")

    try:
        while True:
            event = generate_ride_event()
            try:
                encoded = encode_avro_message(schema_id, schema, event)
                producer.send(TOPIC_NAME, value=encoded)
                print(f"Sent (Avro-encoded, {len(encoded)} bytes): {event['ride_id']}")
            except ValueError as e:
                # This is what happens when an event DOESN'T match the schema --
                # it never gets sent. Try mangling generate_ride_event() to see it.
                print(f"❌ Event rejected by schema, not sent: {e}")

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Avro producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
