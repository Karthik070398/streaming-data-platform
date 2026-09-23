"""
DEBUG TEST: reads raw rows from the 'rides_windowed' Kafka topic with NO
windowing and NO watermark logic -- just proving the connector, JSON
parsing, and timestamp parsing all work correctly on their own.

If this doesn't print anything either, the problem is in the basic Kafka
connection/parsing. If THIS works but the windowed version doesn't, the
problem is specifically in the windowing/watermark logic.

Run it with:
    python3 layer4_late_data/debug_raw_read.py
"""

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

JAR_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "flink_jars", "flink-sql-connector-kafka-3.2.0-1.19.jar")
)


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("pipeline.jars", f"file://{JAR_PATH}")

    # Use a BRAND NEW consumer group name so we're guaranteed to read
    # from the very beginning, with no leftover offset state from
    # earlier test runs interfering.
    table_env.execute_sql("""
        CREATE TABLE rides_raw (
            ride_id STRING,
            pickup_city STRING,
            fare DOUBLE,
            event_time TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'rides_windowed',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-debug-group-001',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    print("Reading RAW rows (no windowing) -- should print immediately, one row per message...\n")
    result = table_env.execute_sql("SELECT ride_id, pickup_city, fare, event_time FROM rides_raw")
    result.print()


if __name__ == "__main__":
    main()
