"""
Sends ride events to the 'rides_windowed' topic, formatted the exact way
Flink expects for timestamp parsing (millisecond precision, ISO-8601 with
a trailing 'Z').

Two modes:
    python3 layer4_late_data/late_event_producer.py normal
        --> sends 6 normal, on-time events, 2 seconds apart

    python3 layer4_late_data/late_event_producer.py late <seconds_ago>
        --> sends ONE event whose event_time is <seconds_ago> seconds in
            the past, simulating a message that took a long time to arrive
            (e.g. a phone that was offline and just reconnected)

Run the Flink job (windowed_job.py) FIRST, in its own terminal, before
running this -- otherwise there's nothing consuming what you send.
"""

import sys
import os
import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC_NAME = "rides_windowed"
KAFKA_BROKER = "localhost:9092"

CITIES = ["Columbus", "Cleveland", "Cincinnati", "Toledo"]


def format_flink_timestamp(dt: datetime) -> str:
    """Formats a datetime the way Flink's JSON ISO-8601 timestamp parser expects:
    millisecond precision, trailing 'Z' for UTC. Example: 2026-09-23T06:57:12.345Z"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_event(event_time: datetime) -> dict:
    return {
        "ride_id": str(uuid.uuid4()),
        "pickup_city": random.choice(CITIES),
        "fare": round(random.uniform(5.0, 85.0), 2),
        "event_time": format_flink_timestamp(event_time),
    }


def send_normal_events(producer):
    print("Sending 6 normal, on-time events, 2 seconds apart...")
    for _ in range(6):
        now = datetime.now(timezone.utc)
        event = build_event(now)
        producer.send(TOPIC_NAME, value=event)
        print(f"Sent (on-time): {event}  [wall clock: {now.isoformat()}]")
        producer.flush()
        time.sleep(2)


def send_late_event(producer, seconds_ago: int):
    now = datetime.now(timezone.utc)
    old_time = now - timedelta(seconds=seconds_ago)
    event = build_event(old_time)
    producer.send(TOPIC_NAME, value=event)
    producer.flush()
    print(f"Sent LATE event: event_time={event['event_time']} "
          f"(claims to be from {seconds_ago}s ago) at wall clock {now.isoformat()}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("normal", "late"):
        print("Usage:")
        print("  python3 late_event_producer.py normal")
        print("  python3 late_event_producer.py late <seconds_ago>")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    if sys.argv[1] == "normal":
        send_normal_events(producer)
    elif sys.argv[1] == "late":
        if len(sys.argv) != 3:
            print("Usage: python3 late_event_producer.py late <seconds_ago>")
            sys.exit(1)
        send_late_event(producer, int(sys.argv[2]))

    producer.close()


if __name__ == "__main__":
    main()
