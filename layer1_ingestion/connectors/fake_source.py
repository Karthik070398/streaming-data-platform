"""
A SOURCE that generates fake ride events -- fulfills the BaseSource contract.

Later, we'll add a postgres_source.py that reads real rows from Postgres
instead. The engine won't care which one it's using.
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from shared.event_generator import generate_ride_event
from layer1_ingestion.connectors.base_connector import BaseSource


class FakeGeneratorSource(BaseSource):
    """Produces one fake ride event each time get_event() is called."""

    def __init__(self, config: dict):
        # config comes straight from the YAML file's "source.options" section.
        # We don't need any options for the fake generator yet, but this is
        # where you'd read things like config.get("rows_per_second").
        self.config = config

    def get_event(self) -> dict:
        return generate_ride_event()
