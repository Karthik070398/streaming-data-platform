"""
The NAIVE consumer -- processes every message it sees, no questions asked.
This is what most beginner Kafka code looks like, and it's exactly why
duplicate messages become duplicate charges, duplicate emails, etc. in
real systems if nobody thinks about this.

Run it with:
    python3 layer3_exactly_once/naive_consumer.py
"""

import json
from kafka import KafkaConsumer

TOPIC_NAME = "rides_chaos"
KAFKA_BROKER = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    processed_count = 0
    seen_ride_ids = set()
    duplicate_count = 0

    print(f"Naive consumer listening on '{TOPIC_NAME}' (Ctrl+C to stop)...")

    try:
        for message in consumer:
            event = message.value
            processed_count += 1

            # We're not skipping duplicates here -- just observing them,
            # so we can report the damage a naive consumer would cause.
            if event["ride_id"] in seen_ride_ids:
                duplicate_count += 1
                print(f"  ⚠️  Processed DUPLICATE ride {event['ride_id']} (charged/counted twice!)")
            else:
                seen_ride_ids.add(event["ride_id"])
                print(f"Processed: {event['ride_id']}")

    except KeyboardInterrupt:
        print(f"\n--- Naive consumer summary ---")
        print(f"Total messages processed: {processed_count}")
        print(f"Unique rides seen:        {len(seen_ride_ids)}")
        print(f"Duplicates processed:     {duplicate_count}  (each one is a bug in a real system)")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
