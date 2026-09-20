"""
STEP 1 SANITY TEST: sends fake ride events into a Kafka topic called "rides".

This is intentionally the simplest possible producer -- no config files,
no YAML, no abstraction. The goal right now is just to prove: my laptop's
Python code can talk to Kafka running inside Docker. Once this works,
we'll generalize it in a later step.

Run it with:  python3 layer1_ingestion/producer.py
"""

import json
import time
import sys
import os

from kafka import KafkaProducer

# Let this script import from the shared/ folder one level up
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.event_generator import generate_ride_event

TOPIC_NAME = "rides"
KAFKA_BROKER = "localhost:9092"


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # Kafka sends raw bytes. This tells it: "turn my Python dict into JSON bytes."
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Connected to Kafka. Sending events to topic '{TOPIC_NAME}'... (Ctrl+C to stop)")

    try:
        while True:
            event = generate_ride_event()
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")
            time.sleep(1)  # one fake ride event per second
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
