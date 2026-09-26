# Streaming Data Platform

<img width="1881" height="826" alt="grafana-dashboard" src="https://github.com/user-attachments/assets/a25ef820-878c-404f-a2e6-202ab2a5030f" />


A hands-on data engineering platform simulating a ride-share event stream, built to
model and solve four real production problems documented in the engineering blogs of
Netflix, LinkedIn, Uber, and Amazon.

**Status:** Complete — all four layers plus an observability stack are implemented
and tested. See [PROGRESS.md](PROGRESS.md) for a detailed build log, including the
real infrastructure issues diagnosed and resolved along the way.

## Problems this solves

| Layer | Problem | Modeled after |
|---|---|---|
| 1. Ingestion | Fragmented, hardcoded pipelines per data source | [Netflix's Data Bridge](https://netflixtechblog.medium.com/data-bridge-how-netflix-simplifies-data-movement-36d10d91c313) |
| 2. Schema Contracts | Uncoordinated schema changes breaking downstream consumers | Kafka Schema Registry patterns ([IBM explainer](https://www.ibm.com/blog/level-up-your-kafka-applications-with-schemas)) |
| 3. Exactly-Once Delivery | Duplicate/lost messages in distributed streaming | [LinkedIn's Brooklin](https://engineering.linkedin.com/blog/2019/brooklin-open-source) |
| 4. Late-Arriving Data | Out-of-order events corrupting real-time aggregates | [Flink watermarking](https://www.ververica.com/blog/watermarks-in-apache-flink-made-easy) |

## Architecture

```
Postgres / fake generator
        │
        ▼
[Layer 1] Config-driven ingestion engine (YAML → source → sink)
        │
        ▼
   Kafka topics (rides, rides_chaos, rides_avro, rides_windowed)
        │
        ├──▶ [Layer 2] Schema Registry + Avro enforcement
        │
        ├──▶ [Layer 3] Idempotent, offset-tracked consumers (exactly-once semantics)
        │
        └──▶ [Layer 4] PyFlink windowed aggregation with event-time watermarking
        │
        ▼
[Observability] Prometheus + Grafana — consumer lag, throughput, cluster health
```

## Tech stack

Apache Kafka, Apache Flink (PyFlink), Confluent Schema Registry, Avro, Postgres,
Python, Docker Compose, Prometheus, Grafana

## Repository structure

```
layer1_ingestion/         Config-driven ingestion engine (source/sink connectors, YAML configs)
layer2_schema_contracts/  Avro schemas, Schema Registry tooling, Avro producer/consumer
layer3_exactly_once/      Chaos producer, naive vs. idempotent consumers
layer4_late_data/         PyFlink windowed job, watermarking, late-event test producers
shared/                   Shared fake event generator
monitoring/               Prometheus config + Grafana dashboard definition
docker-compose.yml        Full infrastructure: Kafka, Zookeeper, Schema Registry,
                          Postgres, Kafka UI, Prometheus, Grafana
PROGRESS.md               Detailed build log and debugging history
```

## Setup

1. Install Docker Desktop, Python 3.11, and Java 11
2. Clone this repo and `cd` into it
3. Create a virtual environment and install dependencies:
   ```
   python3 -m venv venv
   source venv/bin/activate
   python3 -m pip install -r requirements.txt
   ```
4. Start the infrastructure:
   ```
   docker compose up -d
   ```
5. Download the Flink Kafka connector JAR (not committed to this repo — see note below)
   into `flink_jars/`:
   ```
   mkdir -p flink_jars
   curl -o flink_jars/flink-sql-connector-kafka-3.2.0-1.19.jar \
     https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
   ```

## Running each layer

**Layer 1 — config-driven ingestion:**
```
python3 layer1_ingestion/engine.py layer1_ingestion/configs/rides_stream.yaml
```

**Layer 2 — schema enforcement demo:**
```
python3 layer2_schema_contracts/schema_tool.py register rides_avro-value layer2_schema_contracts/schemas/ride_event_v1.avsc
python3 layer2_schema_contracts/send_bad_event_demo.py
```

**Layer 3 — exactly-once processing demo:**
```
python3 layer3_exactly_once/chaos_producer.py
python3 layer3_exactly_once/idempotent_consumer.py
```

**Layer 4 — windowed aggregation with watermarking:**
```
python3 layer4_late_data/windowed_job.py
python3 layer4_late_data/demo_late_data.py
```

**Observability dashboard:**
Open `http://localhost:3000` (login: admin/admin), import
`monitoring/streaming_platform_dashboard.json`, and select your Prometheus data source.

**Kafka UI:** `http://localhost:8080`
**Prometheus:** `http://localhost:9090`

## Data note

This project uses synthetic data generated with the `Faker` library, since real
company data isn't available. The architectural patterns and production problems
modeled are real and documented in the engineering blog posts linked above.

## Build log

See [PROGRESS.md](PROGRESS.md) for a full account of what was built in each layer,
along with the real infrastructure issues diagnosed and fixed — including a Docker
networking misconfiguration, a Python/Cython dependency conflict, and a distributed
watermark-stalling bug caused by a Kafka partition / Flink parallelism mismatch.
