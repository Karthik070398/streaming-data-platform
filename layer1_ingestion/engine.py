"""
THE INGESTION ENGINE.

This is the whole point of Layer 1: one generic program that can run ANY
pipeline, as long as that pipeline is described in a YAML config file and
its source/sink types are registered below.

Run it with:
    python3 layer1_ingestion/engine.py layer1_ingestion/configs/rides_stream.yaml

Compare this to our old producer.py: that script only knew how to do ONE
thing (generate rides, send to Kafka). This script can run infinitely many
different pipelines just by pointing it at a different YAML file -- that's
the "config-driven" pattern that solves the pipeline-sprawl problem.
"""

import sys
import os
import time
import yaml

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from layer1_ingestion.connectors.fake_source import FakeGeneratorSource
from layer1_ingestion.connectors.kafka_sink import KafkaSink

# This is our "registry" -- a lookup table mapping the string in the YAML
# file to the actual Python class that implements it. To add a new source
# or sink type later (e.g. "postgres"), you write the class and add ONE
# line here. engine.py itself never needs to change again.
SOURCE_REGISTRY = {
    "fake_generator": FakeGeneratorSource,
}

SINK_REGISTRY = {
    "kafka": KafkaSink,
}


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def build_source(config: dict):
    source_type = config["source"]["type"]
    source_options = config["source"].get("options", {})
    source_class = SOURCE_REGISTRY[source_type]
    return source_class(source_options)


def build_sink(config: dict):
    sink_type = config["sink"]["type"]
    sink_options = config["sink"].get("options", {})
    sink_class = SINK_REGISTRY[sink_type]
    return sink_class(sink_options)


def run_pipeline(config_path: str):
    config = load_config(config_path)
    pipeline_name = config.get("pipeline_name", "unnamed_pipeline")
    interval = config.get("interval_seconds", 1)

    print(f"Starting pipeline: {pipeline_name}")
    print(f"  Source: {config['source']['type']}")
    print(f"  Sink:   {config['sink']['type']}")

    source = build_source(config)
    sink = build_sink(config)

    try:
        while True:
            event = source.get_event()
            sink.send(event)
            print(f"[{pipeline_name}] moved event: {event}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopping pipeline: {pipeline_name}")
    finally:
        sink.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 engine.py <path_to_config.yaml>")
        sys.exit(1)

    run_pipeline(sys.argv[1])
