"""
Generates fake ride-share events, e.g. {"ride_id": "...", "rider": "...", "fare": 12.50, ...}

Why fake data? Real companies use real production data. We don't have that,
so we simulate it -- this is a totally normal and expected thing to do in a
portfolio project. Just say so clearly in your README (we already will).
"""

import random
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()


def generate_ride_event() -> dict:
    """Returns one fake ride event as a Python dictionary."""
    return {
        "ride_id": str(uuid.uuid4()),
        "rider_name": fake.name(),
        "pickup_city": fake.city(),
        "dropoff_city": fake.city(),
        "fare": round(random.uniform(5.0, 85.0), 2),
        "status": random.choice(["requested", "in_progress", "completed", "cancelled"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
