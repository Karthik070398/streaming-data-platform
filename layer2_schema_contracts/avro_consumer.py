"""
Reads Avro-encoded messages from Kafka, using Schema Registry to figure
out how to decode each one (via the schema ID embedded in the message).

Run it with:
    python3 layer2_schema_contracts/avro_consumer.py
"""

import sys
import os
from kafka import KafkaConsumer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from layer2_schema_contracts.avro_utils import decode_avro_message

TOPIC_NAME = "rides_avro"
KAFKA_BROKER = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,  # raw bytes -- we decode manually
    )

    schema_cache = {}  # avoids re-fetching the same schema from the registry repeatedly

    print(f"Listening for Avro-encoded events on '{TOPIC_NAME}' (Ctrl+C to stop)...")

    try:
        for message in consumer:
            event = decode_avro_message(message.value, schema_cache)
            print(f"Decoded: {event}")
    except KeyboardInterrupt:
        print("\nStopping Avro consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
