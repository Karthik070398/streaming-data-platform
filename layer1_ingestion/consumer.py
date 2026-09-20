"""
STEP 1 SANITY TEST (part 2): reads events back out of the "rides" topic and prints them.

Run this in a SEPARATE terminal window/tab from producer.py, at the same time.
You should see the same events appear here a moment after producer.py sends them.

Run it with:  python3 layer1_ingestion/consumer.py
"""

import json
from kafka import KafkaConsumer

TOPIC_NAME = "rides"
KAFKA_BROKER = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",  # if topic already has messages, read from the start
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"Listening for events on topic '{TOPIC_NAME}'... (Ctrl+C to stop)")

    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
