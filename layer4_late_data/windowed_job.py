"""
THE REAL LAYER 4 JOB.

Reads ride events from Kafka, groups them into 10-second "tumbling"
windows (non-overlapping time buckets) based on event_time (when the ride
ACTUALLY happened), not processing time (when Flink happened to see it).

The WATERMARK clause is the key line:
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

This tells Flink: "I'll tolerate events arriving up to 5 seconds late.
Once I've seen a timestamp T, I consider any window ending before T-5s
to be permanently closed -- anything claiming to belong to it that
arrives after that point gets silently dropped."

Run it with:
    python3 layer4_late_data/windowed_job.py

Leave it running, then in ANOTHER terminal run late_event_producer.py to
feed it data and watch what gets counted vs. dropped.
"""

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

JAR_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "flink_jars", "flink-sql-connector-kafka-3.2.0-1.19.jar")
)


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # IMPORTANT: our Kafka topic has only 1 partition, but Flink defaults to
    # running with multiple parallel workers (often one per CPU core). Any
    # worker that gets no partition assigned never advances its watermark,
    # and Flink takes the MINIMUM watermark across all workers -- so with
    # more workers than partitions, the overall watermark can get stuck
    # forever and no window will ever "close." Since we only have 1
    # partition, we force parallelism down to 1 to match it.
    table_env.get_config().set("parallelism.default", "1")

    # Register our downloaded connector JAR so Flink knows how to talk to Kafka.
    table_env.get_config().set("pipeline.jars", f"file://{JAR_PATH}")

    print(f"Using Kafka connector jar: {JAR_PATH}")
    print(f"  (exists: {os.path.exists(JAR_PATH)})")

    # The Kafka SOURCE table -- this is where the watermark is defined.
    table_env.execute_sql("""
        CREATE TABLE rides_windowed (
            ride_id STRING,
            pickup_city STRING,
            fare DOUBLE,
            event_time TIMESTAMP_LTZ(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'rides_windowed',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-windowed-job-v2',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    print("Starting windowed aggregation -- counting rides per city per 10-second window...")
    print("(Leave this running. Results print below as each window closes.)\n")

    # TUMBLE = non-overlapping, fixed-size time windows (10 seconds each).
    # A row only prints here once Flink considers a window fully closed --
    # i.e., once the watermark has passed that window's end time.
    result = table_env.execute_sql("""
        SELECT
            window_start,
            window_end,
            pickup_city,
            COUNT(*) AS ride_count
        FROM TABLE(
            TUMBLE(TABLE rides_windowed, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)
        )
        GROUP BY window_start, window_end, pickup_city
    """)

    result.print()


if __name__ == "__main__":
    main()
