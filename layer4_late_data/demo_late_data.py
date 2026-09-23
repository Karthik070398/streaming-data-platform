"""
A SELF-CONTAINED late-data demo. Unlike late_event_producer.py's "late N"
mode (which computes lateness from your computer's WALL CLOCK -- unreliable
if there's been a long gap since you last sent data), this script computes
lateness relative to timestamps it JUST sent, guaranteeing a genuinely late
event regardless of how much real time has passed since your last test.

Sequence:
  1. Send 3 "anchor" events, 2 seconds apart, in ONE city (so we get a
     clean, easy-to-spot window with a known count).
  2. Send 3 "pusher" events in a DIFFERENT city, with event_time set to
     a large FIXED offset ahead of the anchor timestamps (not "now") --
     this deterministically forces the watermark past the anchor window,
     regardless of how fast this script actually runs in real time.
  3. NOW send ONE event whose event_time is 30 seconds BEFORE the last
     anchor event. Since the anchor window is already closed (step 2),
     this event should be silently dropped -- proving the watermark
     boundary is real and enforced.
  4. Send one final pusher to confirm everything has settled.

Run it with:
    python3 layer4_late_data/demo_late_data.py

Watch the Flink job's terminal (windowed_job.py must already be running)
for the results.
"""

import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC_NAME = "rides_windowed"
KAFKA_BROKER = "localhost:9092"
DEMO_CITY = "Capital City"  # a fresh, never-used city name so results are unambiguous


def format_flink_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_event(event_time: datetime, city: str = DEMO_CITY) -> dict:
    return {
        "ride_id": str(uuid.uuid4()),
        "pickup_city": city,
        "fare": round(random.uniform(5.0, 85.0), 2),
        "event_time": format_flink_timestamp(event_time),
    }


def send(producer, event):
    producer.send(TOPIC_NAME, value=event)
    producer.flush()
    print(f"Sent: pickup_city={event['pickup_city']:<12} event_time={event['event_time']}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("=== Step 1: sending 3 ANCHOR events (on-time), 2 seconds apart ===")
    anchor_time = None
    for _ in range(3):
        now = datetime.now(timezone.utc)
        anchor_time = now  # remember the LAST anchor timestamp
        send(producer, build_event(now))
        time.sleep(2)

    print(f"\n=== Step 2: sending 3 PUSHER events (different city), to force the watermark PAST the anchor window ===")
    # IMPORTANT: these use event_time = anchor_time + a large fixed offset,
    # NOT "now" -- this guarantees they push the watermark decisively past
    # the anchor window regardless of how fast or slow this script actually
    # runs in real wall-clock time.
    for offset in (60, 70, 80):
        pusher_time = anchor_time + timedelta(seconds=offset)
        send(producer, build_event(pusher_time, city="Shelbyville"))
        time.sleep(1)

    print(f"\n=== Step 3: NOW sending the LATE event, 30 seconds before the last anchor ===")
    print("    (The anchor window is now guaranteed closed, thanks to Step 2's far-future pushers.)")
    late_time = anchor_time - timedelta(seconds=30)
    late_event = build_event(late_time)
    send(producer, late_event)
    print(f"    (This claims to be from {late_time.isoformat()}, which is BEFORE the anchor")
    print(f"     events at ~{anchor_time.isoformat()}.)")

    print(f"\n=== Step 4: sending 1 more PUSHER event, just to flush/confirm final state ===")
    final_pusher_time = anchor_time + timedelta(seconds=90)
    send(producer, build_event(final_pusher_time, city="Shelbyville"))

    print("\nDone. Check the Flink job's terminal now.")
    print(f"Look for a window containing '{DEMO_CITY}':")
    print(f"  - You SHOULD see a row for the anchor window with count=3 (the late event should NOT be added to it)")
    print(f"  - You should NOT see any row with count=4 for that same window")

    producer.close()


if __name__ == "__main__":
    main()
