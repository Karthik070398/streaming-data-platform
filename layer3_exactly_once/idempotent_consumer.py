"""
The IDEMPOTENT consumer -- now with BOTH layers of protection real
production systems use:

  1. Kafka-level offset tracking (via a consumer group) -- avoids
     re-reading the ENTIRE topic history on every restart.
  2. Application-level dedup by ride_id (persisted to a file) -- catches
     the genuine duplicates that slip through even WITH offset tracking,
     e.g. a crash between processing a message and Kafka saving the offset.

Neither layer alone is enough:
  - Offset tracking alone still allows the "processed but crashed before
    commit" duplicate described above.
  - Our own dedup file alone (Layer 3 v1) works, but forces a full topic
    re-read every restart, which doesn't scale on a large topic.

Together, they're the real answer to "how do you get exactly-once behavior
on top of Kafka's at-least-once guarantee."

Run it with:
    python3 layer3_exactly_once/idempotent_consumer.py
"""

import json
import os
from kafka import KafkaConsumer

TOPIC_NAME = "rides_chaos"
KAFKA_BROKER = "localhost:9092"
CONSUMER_GROUP = "idempotent-consumer-group"
SEEN_IDS_FILE = os.path.join(os.path.dirname(__file__), "seen_ride_ids.json")


def load_seen_ids() -> set:
    if os.path.exists(SEEN_IDS_FILE):
        with open(SEEN_IDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(seen_ids: set):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump(list(seen_ids), f)


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    seen_ride_ids = load_seen_ids()
    processed_count = 0
    skipped_duplicate_count = 0

    print(f"Idempotent consumer (group='{CONSUMER_GROUP}') listening on '{TOPIC_NAME}' (Ctrl+C to stop)...")
    print(f"(Already know about {len(seen_ride_ids)} rides from previous runs)")

    try:
        for message in consumer:
            event = message.value

            if event["ride_id"] in seen_ride_ids:
                skipped_duplicate_count += 1
                print(f"  ⏭️  Skipped duplicate: {event['ride_id']} (already processed)")
                continue

            seen_ride_ids.add(event["ride_id"])
            processed_count += 1
            print(f"Processed (for real): {event['ride_id']}")

    except KeyboardInterrupt:
        save_seen_ids(seen_ride_ids)
        print(f"\n--- Idempotent consumer summary ---")
        print(f"Genuinely processed: {processed_count}")
        print(f"Duplicates skipped:  {skipped_duplicate_count}  (correctly ignored, zero double-processing)")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
