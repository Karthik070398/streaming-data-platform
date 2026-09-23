# Project Progress Log

A running record of what's been built, the real problems solved, and the debugging
lessons learned along the way. Kept alongside the README as a more detailed,
narrative account of the project's development.

---

## Environment Setup

Tools installed and configured: Git, GitHub CLI, Docker Desktop, Python 3.11
(via a dedicated virtual environment), Java 11, and PyFlink.

**Real issues hit and fixed:**
- Conda's `(base)` environment silently shadowing the project's `venv`, causing
  `pip install` to install into the wrong Python — fixed by disabling Conda's
  auto-activation and being explicit about `which python3` / `which pip`.
- `psycopg2-binary` and other packages failing to build on Python 3.14 (too new,
  no pre-built wheels yet) — fixed by installing Python 3.11 specifically via
  Homebrew and rebuilding the venv against it.
- GitHub rejecting plain password authentication on `git push` — fixed by
  installing and authenticating with the GitHub CLI (`gh auth login`).
- Kafka UI spinning forever with no topics showing, caused by Kafka only
  advertising a `localhost`-only listener — other containers on Docker's
  internal network (like Kafka UI) couldn't resolve `localhost` to the actual
  Kafka container. Fixed with a dual-listener Kafka config: one address for
  other containers, one for the host machine.

---

## Layer 1: Config-Driven Ingestion Engine

**Problem modeled:** Netflix's "Data Bridge" — pipeline sprawl from building a
custom script per data source/destination pair.

**Built:**
- `BaseSource` / `BaseSink` interfaces (a shared contract any connector follows)
- `FakeGeneratorSource` and `KafkaSink` implementing that contract
- `engine.py` — a generic engine that reads a YAML config file and runs
  whatever source-to-sink pipeline it describes, with zero new code required
  per pipeline

**Proof it worked:** duplicated `rides_stream.yaml` into `orders_stream.yaml`,
changed the topic name, and ran the same `engine.py` against it — it correctly
started writing to a brand-new Kafka topic it had never touched before, with
no code changes.

---

## Layer 2: Schema Contracts

**Problem modeled:** schema drift silently breaking downstream Kafka consumers
(the class of problem Confluent Schema Registry and similar tools at
Uber-scale companies are built to prevent).

**Built:**
- Avro schemas (`ride_event_v1.avsc`, a safe evolution, and a breaking one)
- `schema_tool.py` — registers schemas and checks compatibility against
  Schema Registry's REST API
- An Avro-encoding producer/consumer pair (`avro_producer.py`,
  `avro_consumer.py`) that enforce the schema at message-build time, not just
  at schema-registration time
- `send_bad_event_demo.py` — proves a malformed event (wrong type, missing
  field) is rejected before it ever reaches Kafka

**Key lesson proven:** backward compatibility means "new code must be able to
read old data." Adding a field needs a default value or it breaks; removing a
field is usually safe. Verified both directions concretely — one schema
change accepted, one rejected.

---

## Layer 3: Exactly-Once Processing

**Problem modeled:** LinkedIn's Kafka/Brooklin work on reliable, high-volume
message delivery, and the general challenge of Kafka's at-least-once
guarantee causing duplicate processing.

**Built:**
- `chaos_producer.py` — deliberately resends ~15% of messages, simulating a
  real network retry
- `naive_consumer.py` — processes every message with no deduplication,
  demonstrating the resulting bug
- `idempotent_consumer.py` — deduplicates by `ride_id`, with state persisted
  to disk so it survives restarts

**Refinement added:** both consumers were upgraded to use real Kafka consumer
groups (offset tracking), after recognizing the original version only proved
idempotency against a full topic replay, not the more realistic
"crash-after-processing-but-before-offset-commit" scenario. Verified both
failure modes separately.

**Key lesson proven:** Kafka guarantees at-least-once delivery, never exactly
once. Real exactly-once *behavior* comes from designing idempotent consumers
that deduplicate by a unique key, with that dedup state persisted — proven by
comparing a naive consumer's duplicate-processing bug against a fixed one,
across both a full-replay scenario and a normal-restart scenario.

---

## Layer 4: Late-Arriving Data & Watermarking

**Problem modeled:** Amazon-style late/out-of-order event handling in
streaming pipelines, using Apache Flink's event-time and watermark model.

**Built:**
- `hello_flink.py` — isolated sanity test proving PyFlink + Java work
  correctly before adding any Kafka complexity
- `windowed_job.py` — a PyFlink Table API job reading from Kafka with a real
  watermark strategy (`WATERMARK FOR event_time AS event_time - INTERVAL '5'
  SECOND`), aggregating ride counts per city in 10-second tumbling windows
- `late_event_producer.py` / `demo_late_data.py` — producers capable of
  sending deliberately out-of-order ("late") events to test the pipeline

**Real issues hit and fixed (the hardest layer by far):**
- `apache-beam`'s old, pinned dependency chain failing to build against a
  newer Cython — fixed by pinning `cython<3` and installing with
  `--no-build-isolation`.
- **The parallelism/idle-watermark bug:** Flink defaulted to running with
  more parallel workers than the Kafka topic had partitions. Workers with no
  partition assigned never advanced their watermark, and since Flink takes
  the *minimum* watermark across all workers, the overall watermark got
  permanently stuck — meaning no window would ever close, no matter how much
  data flowed through the one active worker. Fixed by forcing
  `parallelism.default = 1` to match the single-partition topic.
- **Wall-clock vs. event-time confusion:** an early test computed "late" as
  "60 seconds before wall-clock now," which produced misleading results after
  long real-world gaps (from debugging), since the watermark only advances
  based on timestamps actually seen in the data — not real time passing. This
  was fixed by building a fully deterministic test where every timestamp
  (anchors, pushers, and the late event) is calculated relative to other
  timestamps already sent, never relative to the wall clock.

**Key lesson proven:** a late-arriving event does not corrupt an
already-closed window's result. An anchor window's count stayed correctly at
3 even after a deliberately "late" event (claiming a timestamp 30 seconds
earlier) was sent — proven only after first forcing the watermark past that
window using event-time-based "pusher" events, decoupled entirely from real
wall-clock timing.

---

## What this project demonstrates

Beyond the four architectural patterns themselves, this project involved
genuinely diagnosing and fixing five distinct categories of real
infrastructure problems: a Python environment conflict, a Docker networking
misconfiguration, a GitHub authentication change, a Python/Cython version
incompatibility, and a distributed systems watermark-stalling bug. That
combination — building the thing, and correctly debugging it when it breaks
in non-obvious ways — is the core of real data engineering work, not just
the pipeline code itself.

## Roadmap

- [x] Environment setup (Git, Docker, Python 3.11, Java 11, PyFlink)
- [x] Layer 1: config-driven ingestion engine
- [x] Layer 2: schema contracts + Avro enforcement
- [x] Layer 3: exactly-once processing via idempotent, offset-tracked consumers
- [x] Layer 4: windowed aggregation with watermarking for late data
- [ ] Observability: Prometheus + Grafana dashboard across all four layers
- [ ] Optional: data catalog (Airbnb's Dataportal-style discovery layer)
