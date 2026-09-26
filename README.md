# Streaming Data Platform

A hands-on data engineering platform that simulates a ride-share event stream and
solves four real production problems documented in top tech companies' engineering blogs.

> **Status:** 🚧 Work in progress — building layer by layer. See commit history for progress.

## Problems this solves

| Layer | Problem | Modeled after |
|---|---|---|
| 1. Ingestion | Fragmented, hardcoded pipelines per data source | [Netflix's Data Bridge](https://netflixtechblog.medium.com/data-bridge-how-netflix-simplifies-data-movement-36d10d91c313) |
| 2. Schema Contracts | Uncoordinated schema changes breaking downstream consumers | Kafka Schema Registry patterns ([IBM explainer](https://www.ibm.com/blog/level-up-your-kafka-applications-with-schemas)) |
| 3. Exactly-Once Delivery | Duplicate/lost messages in distributed streaming | [LinkedIn's Brooklin](https://engineering.linkedin.com/blog/2019/brooklin-open-source) |
| 4. Late-Arriving Data | Out-of-order events corrupting real-time aggregates | [Flink watermarking](https://www.ververica.com/blog/watermarks-in-apache-flink-made-easy) |

## Architecture

```
Postgres (fake production DB)
        │
        ▼
   [Layer 1: Ingestion Engine]  ──▶  Kafka topic: rides
        │
        ▼
   [Layer 2: Schema Registry]   (validates message shape)
        │
        ▼
   [Layer 3: Exactly-once consumer]  (idempotent, no duplicates)
        │
        ▼
   [Layer 4: Flink/streaming job]  (handles late/out-of-order events)
```

## Tech stack

Kafka, Zookeeper, Schema Registry, Postgres, Python, Docker Compose, (later: Flink, Grafana)

## Setup

1. Install Docker Desktop and Python 3.11+
2. Clone this repo and `cd` into it
3. Install Python dependencies:
   ```
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
4. Start the infrastructure:
   ```
   docker compose up -d
   ```
5. Open http://localhost:8080 to see the Kafka UI dashboard
6. In one terminal, run the producer:
   ```
   python3 layer1_ingestion/producer.py
   ```
7. In another terminal, run the consumer:
   ```
   python3 layer1_ingestion/consumer.py
   ```

You should see fake ride events flowing from the producer to the consumer in real time.

## Data note

This project uses synthetic (fake) data generated with the `Faker` library, since
real company data isn't available. All patterns and problems modeled are real and
documented in the engineering blog posts linked above.

## Roadmap

- [x] Kafka + Postgres infrastructure running in Docker
- [x] Basic producer/consumer proving connectivity
- [x] Layer 1: config-driven ingestion engine (YAML-defined sources/sinks)
- [x] Layer 2: Schema Registry enforcement + Avro-based encoding, with compatibility demo
- [x] Layer 3: exactly-once processing via idempotent, offset-tracked consumers
- [x] Layer 4: PyFlink windowed aggregation with watermarking for late-arriving data
- [x] Observability: Prometheus + Grafana dashboard for consumer lag and throughput
- [ ] Optional: data catalog (Airbnb's Dataportal-style discovery layer)

## Observability

A Grafana dashboard (backed by Prometheus + kafka-exporter) tracks:
- Consumer group lag, per topic and partition
- Message throughput per topic
- Consumer group offset progress
- Kafka broker and partition health

Access it at `http://localhost:3000` (admin/admin) once `docker compose up -d` is running.
Dashboard definition: `monitoring/streaming_platform_dashboard.json` (import via
Grafana's Dashboards → Import screen).