"""
A SINK that writes events to a Kafka topic -- fulfills the BaseSink contract.

This is basically the same Kafka logic from our original producer.py,
just wrapped so the engine can use it generically.
"""

import json
import sys
import os

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from layer1_ingestion.connectors.base_connector import BaseSink


class KafkaSink(BaseSink):
    """Sends event dictionaries as JSON to a Kafka topic."""

    def __init__(self, config: dict):
        # config comes from the YAML file's "sink.options" section, e.g.:
        #   options:
        #     topic: rides
        #     bootstrap_servers: localhost:9092
        self.topic = config["topic"]
        bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, event: dict) -> None:
        self.producer.send(self.topic, value=event)

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
