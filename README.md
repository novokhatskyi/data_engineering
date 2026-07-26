# Data Engineering — Streaming & Batch Pipelines

Portfolio of data-engineering work built around a production-shaped stack:
**Apache Kafka · Apache Spark (Structured Streaming & PySpark) · Apache Airflow · Docker · MySQL**.

The centrepiece is a two-part final project that implements both processing paradigms
side by side — a real-time streaming pipeline and a batch multi-hop Data Lake — followed by
five focused projects covering individual pieces of the stack.

---

## Tech stack

| Layer | Tools |
|---|---|
| Streaming | Apache Kafka (SASL auth), Spark Structured Streaming |
| Batch processing | PySpark, Spark SQL |
| Orchestration | Apache Airflow (SparkSubmitOperator, BranchPythonOperator, SqlSensor, trigger rules) |
| Storage | MySQL, Parquet, multi-hop Data Lake (Landing → Bronze → Silver → Gold) |
| Infrastructure | Docker, Docker Compose |
| Language | Python 3 |

---

## Final Project — Streaming and Batch Pipelines

📁 [`final_project/`](./final_project) · [detailed write-up](./final_project/README.md)

Two approaches to the same domain (Olympic athlete data), built to contrast real-time and batch processing.

### Part 1 — Streaming pipeline

```
MySQL (athlete_bio) ─┐
                     ├─► Spark Structured Streaming ─► aggregation ─► Kafka topic + MySQL sink
Kafka (event_results)┘
```

Competition results arrive continuously through a Kafka topic and are enriched in-flight with
athlete biometric data read from MySQL. The stream joins both sources, normalises the `medal`
field (nulls, empty strings and literal `"nan"` collapse into `No medal`), then aggregates
average height and weight grouped by sport, medal type, sex and country — producing features
ready for downstream ML consumption, with a `calculated_at` timestamp on every record.

The processing logic is split into composable, individually testable functions
(`join_events_with_bio`, `normalize_medal`, `aggregate_athlete_stats`) rather than a single
monolithic script.

### Part 2 — Batch Data Lake with Airflow

```
Landing (CSV) ──► Bronze (raw Parquet) ──► Silver (cleaned, deduplicated) ──► Gold (aggregated)
                              orchestrated by Airflow
```

A medallion-architecture Data Lake where each layer is a separate Spark job submitted by an
Airflow DAG through `SparkSubmitOperator`, with strict sequential dependencies. Bronze stores
raw ingested data as Parquet, Silver applies text cleaning and deduplication, Gold produces the
analytical aggregates. The whole environment runs in Docker Compose.

---

## Projects

### 📊 [`spark-sales-analytics/`](./spark-sales-analytics) — PySpark analytics on retail data

Analytical processing of three related datasets (users, purchases, products) with PySpark:
schema inspection, null-filtering, joins across all three tables, and aggregation of purchase
totals by product category and by customer age bracket. Results rendered as formatted console
tables.

**Stack:** PySpark · Spark SQL

---

### ⚙️ [`spark-ui-optimization/`](./spark-ui-optimization) — Spark UI and execution analysis

Three Spark scripts examined through the Spark UI to understand how the engine actually executes
a job: stages, shuffles, partitioning and physical plans. Includes a written analysis of the
observed execution behaviour.

**Stack:** PySpark · Spark UI

---

### 🔄 [`kafka-streaming-pipeline/`](./kafka-streaming-pipeline) — Kafka producer/consumer pipeline

An end-to-end Kafka pipeline: topic creation, a producer publishing records into an input topic,
a processing stage that consumes, transforms and republishes to an output topic, and a final
consumer reading the result. Runs against a SASL-authenticated broker with an explicit consumer
group and offset configuration.

**Stack:** Kafka (kafka-python) · SASL_PLAINTEXT

---

### 🌡️ [`iot-streaming-alerts/`](./iot-streaming-alerts) — Real-time IoT alerting

Simulated building sensors publish temperature and humidity readings to Kafka. A Spark
Structured Streaming job reads the stream, applies windowed aggregation, and cross-joins the
result against alert thresholds loaded from a CSV configuration file — so alert rules can change
without touching the streaming code. Triggered alerts are written back to a dedicated Kafka topic.

**Stack:** Kafka · Spark Structured Streaming · windowed aggregations

---

### 🗓️ [`airflow-medals-dag/`](./airflow-medals-dag) — Airflow DAG with branching and sensors

A scheduled Airflow DAG demonstrating non-linear control flow: creates the database schema,
randomly picks a medal type, branches into one of three counting tasks via
`BranchPythonOperator`, introduces a variable delay, then uses `SqlSensor` to verify the freshness
of the resulting record. Trigger rules ensure downstream tasks run correctly despite skipped
branches.

**Stack:** Airflow · MySQL · BranchPythonOperator · SqlSensor · trigger rules

---

## Repository layout

```
.
├── final_project/
│   ├── first_part/          # Kafka → Spark Streaming → MySQL/Kafka
│   └── second_part/         # Airflow-orchestrated Data Lake (Bronze/Silver/Gold)
├── goit-de-hw-03/           # PySpark retail analytics
├── goit-de-hw-04/           # Spark UI / execution analysis
├── goit-de-hw-05/           # Kafka pipeline
├── goit-de-hw-06/           # IoT streaming alerts
└── goit-de-hw-07/           # Airflow DAG with branching
```

## Running the projects

Each project runs independently. Kafka-based projects require broker credentials — copy
`.env.example` to `.env` and provide your own values; no credentials are committed to this
repository. The final project's second part ships with a `docker-compose.yaml` that brings up
Airflow and Spark together.

```bash
pip install -r requirements.txt
cp .env.example .env        # then fill in your broker / database settings
```

---

**Author:** Oleksandr Novokhatskyi · [github.com/novokhatskyi](https://github.com/novokhatskyi)
