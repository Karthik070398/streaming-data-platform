"""
SANITY TEST: proves PyFlink + Java are installed and working correctly,
with zero Kafka involvement. If this doesn't run, we fix the PyFlink/Java
setup BEFORE touching anything Kafka-related.

Run it with:
    python3 layer4_late_data/hello_flink.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # This sets up a Flink "environment" -- think of it as turning on the
    # Flink engine itself, in batch-friendly streaming mode.
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Create a tiny, fake, in-memory table -- no Kafka, no files, nothing
    # external. Just to prove Flink itself can process data end to end.
    table_env.execute_sql("""
        CREATE TABLE fake_rides (
            city STRING,
            fare DOUBLE
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '5'
        )
    """)

    print("Running a tiny Flink query on generated fake data...")
    result = table_env.execute_sql("SELECT city, fare FROM fake_rides")
    result.print()


if __name__ == "__main__":
    main()
