"""
Simulates what REALLY happens on a flaky network: sometimes a producer
sends a message, doesn't get a clean confirmation back, and resends it
"just in case." This is not a bug -- it's literally how Kafka's default
at-least-once guarantee works in the real world.

We fake this by deliberately sending ~15% of messages TWICE.

Run it with:
    python3 layer3_exactly_once/chaos_producer.py
"""

import json
import random
import sys
import os
import time

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.event_generator import generate_ride_event

TOPIC_NAME = "rides_chaos"
KAFKA_BROKER = "localhost:9092"
DUPLICATE_RATE = 0.15  # 15% of events get sent twice, simulating a retry


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending events to '{TOPIC_NAME}' with simulated duplicates (Ctrl+C to stop)...")
    sent_count = 0

    try:
        while True:
            event = generate_ride_event()
            producer.send(TOPIC_NAME, value=event)
            sent_count += 1
            print(f"Sent: {event['ride_id']}")

            # Simulate a "retry" -- the same event, sent again, as if the
            # producer never got confirmation the first one arrived.
            if random.random() < DUPLICATE_RATE:
                producer.send(TOPIC_NAME, value=event)
                sent_count += 1
                print(f"  ↳ Simulated retry, RESENT: {event['ride_id']}")

            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\nStopped. Total messages actually sent (including duplicates): {sent_count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
