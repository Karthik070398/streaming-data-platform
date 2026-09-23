"""
The NAIVE consumer -- now using a real Kafka CONSUMER GROUP, which is how
production consumers actually work: Kafka remembers which messages this
group has already read (its "offset"), so a restart continues from where
you left off instead of re-reading the whole topic every time.

IMPORTANT: even with proper offset tracking, duplicates can STILL happen.
Here's the classic real-world sequence:
  1. Consumer reads a message and processes it (e.g., "charge the card")
  2. Consumer crashes BEFORE Kafka saves ("commits") the new offset
  3. Consumer restarts -- Kafka thinks that message was never read,
     so it delivers it again -- and it gets processed a second time.

This is exactly why idempotent processing matters even in a "correctly
configured" production system, not just in our earlier full-replay demo.

Run it with:
    python3 layer3_exactly_once/naive_consumer.py
"""

import json
from kafka import KafkaConsumer

TOPIC_NAME = "rides_chaos"
KAFKA_BROKER = "localhost:9092"
CONSUMER_GROUP = "naive-consumer-group"


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,           # <-- Kafka now tracks our offset under this name
        auto_offset_reset="earliest",       # only matters the FIRST time this group ever runs
        enable_auto_commit=True,            # Kafka periodically saves our progress automatically
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    processed_count = 0
    seen_ride_ids = set()
    duplicate_count = 0

    print(f"Naive consumer (group='{CONSUMER_GROUP}') listening on '{TOPIC_NAME}' (Ctrl+C to stop)...")
    print("Note: run this a 2nd time with nothing new sent -- it should see almost nothing,")
    print("since Kafka remembers this group already read everything. Duplicates now only")
    print("come from genuine retries, not full re-reads.")

    try:
        for message in consumer:
            event = message.value
            processed_count += 1

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
