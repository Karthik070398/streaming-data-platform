"""
The IDEMPOTENT consumer -- this is the realistic fix used in production
systems. "Idempotent" means: processing the same event twice has the
SAME effect as processing it once. We achieve this by remembering every
ride_id we've already handled, and skipping anything we've seen before.

We save seen IDs to a file so this works even if you stop and restart
the consumer -- a real system needs this to survive crashes/restarts too,
not just handle duplicates that arrive back-to-back.

Run it with:
    python3 layer3_exactly_once/idempotent_consumer.py
"""

import json
import os
import sys
from kafka import KafkaConsumer

TOPIC_NAME = "rides_chaos"
KAFKA_BROKER = "localhost:9092"
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
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    seen_ride_ids = load_seen_ids()
    processed_count = 0
    skipped_duplicate_count = 0

    print(f"Idempotent consumer listening on '{TOPIC_NAME}' (Ctrl+C to stop)...")
    print(f"(Already know about {len(seen_ride_ids)} rides from previous runs)")

    try:
        for message in consumer:
            event = message.value

            if event["ride_id"] in seen_ride_ids:
                skipped_duplicate_count += 1
                print(f"  ⏭️  Skipped duplicate: {event['ride_id']} (already processed)")
                continue

            # This is the ONE place real processing would happen --
            # e.g., charging a card, updating a database, sending an email.
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
