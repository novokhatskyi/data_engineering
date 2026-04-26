# Final Project: Streaming and Batch Data Pipelines with Spark and Airflow

## Project Overview

This repository contains a two-part final Data Engineering project.

The project demonstrates two different approaches to data processing:

1. **Part 1: Streaming Pipeline**
   - Real-time processing of athlete event results from Kafka.
   - Enrichment with athlete bio data from MySQL.
   - Generation of aggregated statistics for downstream ML use cases.

2. **Part 2: Batch Data Lake**
   - Batch processing of athlete datasets from CSV files.
   - Construction of a simple multi-hop Data Lake architecture.
   - Data transformation through Landing, Bronze, Silver, and Gold layers.
   - Workflow orchestration with Apache Airflow.

The project uses Apache Spark, Kafka, MySQL, and Apache Airflow.

---

# Part 1: Streaming Pipeline

## Business Context

The company needs to generate features for ML models as quickly as possible.

Some features, such as average height, average weight, sex, and country of origin, are based on athlete bio data. This data is known in advance and stored in a MySQL database.

Competition results arrive continuously through a Kafka topic. The goal is to process these results in streaming mode, join them with athlete bio data, aggregate the information, and produce statistics that can be used for ML models.

## Data Sources

### MySQL Table

```text
olympic_dataset.athlete_bio
```

This table contains athlete profile information, including:

```text
athlete_id
name
sex
height
weight
country
country_noc
```

### Kafka Topic

```text
athlete_event_results
```

This topic contains live event result data, including:

```text
athlete_id
sport
medal
event
result_id
country_noc
```

## Streaming Pipeline Logic

The streaming pipeline performs the following steps:

```text
MySQL athlete_bio
        +
Kafka athlete_event_results
        ↓
Spark Structured Streaming
        ↓
Join by athlete_id
        ↓
Clean and transform height / weight
        ↓
Group by sport, medal, sex, country_noc
        ↓
Calculate average height and average weight
        ↓
Write aggregated statistics to output storage / Kafka
```

## Main Transformations

The pipeline:

- reads athlete bio data from MySQL using Spark JDBC;
- reads athlete event results from Kafka;
- parses Kafka messages from JSON;
- joins event results with athlete bio data by `athlete_id`;
- cleans numeric columns such as `height` and `weight`;
- replaces commas in numeric strings with dots;
- casts `height` and `weight` to numeric types;
- removes invalid or unrealistic values;
- calculates aggregated statistics by sport, medal, sex, and country.

## Data Cleaning Logic

The athlete bio table requires cleaning before aggregation.

The main cleaning rules are:

```text
1. Remove duplicate records.
2. Remove rows with empty height or weight.
3. Replace commas in weight values with dots.
4. Cast height and weight to DoubleType.
5. Drop rows where height or weight cannot be converted.
6. Keep only realistic height values.
7. Keep only realistic weight values.
```

Example logic:

```python
df_bio_table = df_bio_table.select("*") \
    .distinct() \
    .where(trim(col("height")) != "") \
    .where(trim(col("weight")) != "") \
    .withColumn("weight", regexp_replace(col("weight"), ",", ".")) \
    .withColumn("height", col("height").cast(DoubleType())) \
    .withColumn("weight", col("weight").cast(DoubleType())) \
    .dropna(subset=["height", "weight"]) \
    .filter((col("height") >= 100) & (col("height") <= 250)) \
    .filter((col("weight") >= 25) & (col("weight") <= 250))
```

## Expected Output

The final streaming output contains aggregated athlete statistics.

Expected columns:

```text
sport
medal
sex
country_noc
avg_weight
avg_height
timestamp
```

These statistics can be used as features for ML models or as analytical output for downstream systems.

---

# Part 2: Batch Data Lake

## Business Context

The second part of the project focuses on batch data processing.

The goal is to build a simple multi-hop Data Lake using Spark and Airflow. The project processes the same Olympic athlete datasets, but this time the data is loaded from CSV files and processed in batch mode.

The pipeline follows a classic Data Lake architecture:

```text
Landing → Bronze → Silver → Gold
```

## Data Sources

The project uses two CSV datasets provided by GoIT:

```text
https://ftp.goit.study/neoversity/athlete_bio.csv
https://ftp.goit.study/neoversity/athlete_event_results.csv
```

Source tables:

```text
athlete_bio
athlete_event_results
```

## Project Structure

```text
second_part/
├── dags/
│   └── project_solution.py
├── jobs/
│   ├── landing_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── data/
│   ├── landing/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Data Lake Layers

## Landing Layer

The Landing layer stores raw CSV files exactly as they are downloaded from the source.

Expected files:

```text
data/landing/athlete_bio.csv
data/landing/athlete_event_results.csv
```

No transformations are applied at this stage.

---

## Bronze Layer

The Bronze layer stores the raw data in Parquet format.

The `landing_to_bronze.py` Spark job:

- downloads CSV files from the source URLs;
- saves the raw CSV files into the Landing layer;
- reads the CSV files using Spark;
- writes the data in Parquet format into the Bronze layer;
- prints the final DataFrame with `df.show()` so the result is visible in Airflow task logs.

Expected output:

```text
data/bronze/athlete_bio
data/bronze/athlete_event_results
```

---

## Silver Layer

The Silver layer stores cleaned and deduplicated data.

The `bronze_to_silver.py` Spark job:

- reads Parquet data from the Bronze layer;
- applies text cleaning to all string columns;
- removes duplicate rows;
- writes the cleaned data into the Silver layer;
- prints the final DataFrame with `df.show()` so the result is visible in Airflow task logs.

Expected output:

```text
data/silver/athlete_bio
data/silver/athlete_event_results
```

Text cleaning function:

```python
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))
```

---

## Gold Layer

The Gold layer contains the final analytical dataset.

The `silver_to_gold.py` Spark job:

- reads both Silver tables:
  - `silver/athlete_bio`
  - `silver/athlete_event_results`
- joins them by `athlete_id`;
- cleans and converts `height` and `weight` to numeric types;
- groups data by:
  - `sport`
  - `medal`
  - `sex`
  - `country_noc`
- calculates average `weight` and average `height`;
- adds a processing timestamp;
- writes the final result to the Gold layer;
- prints the final DataFrame with `df.show()` so the result is visible in Airflow task logs.

Expected output:

```text
data/gold/avg_stats
```

Final columns:

```text
sport
medal
sex
country_noc
avg_weight
avg_height
timestamp
```

---

# Airflow Orchestration

## DAG File

```text
dags/project_solution.py
```

The Airflow DAG orchestrates all three Spark jobs in the correct order:

```text
landing_to_bronze → bronze_to_silver → silver_to_gold
```

The DAG uses `SparkSubmitOperator` to run Spark jobs.

## DAG Name

```text
final_project_batch_datalake
```

## Spark Connection

Airflow requires a Spark connection:

```text
Connection Id: spark-default
Connection Type: Spark
Host: local[*]
```

This configuration allows Airflow to run Spark jobs locally inside the Docker environment.

---

# Running the Batch Pipeline Locally

## 1. Build Docker Image

```bash
docker compose build
```

## 2. Start Airflow

```bash
docker compose up -d
```

## 3. Open Airflow UI

```text
http://localhost:8080
```

Default credentials:

```text
Username: airflow
Password: airflow
```

## 4. Trigger the DAG

In the Airflow UI:

1. Find the DAG:

```text
final_project_batch_datalake
```

2. Enable it.
3. Trigger it manually.
4. Check that all tasks finish successfully.

Expected task order:

```text
landing_to_bronze
bronze_to_silver
silver_to_gold
```

---

# Requirements

Main dependencies:

```text
apache-airflow-providers-apache-spark
pyspark
requests
```

These dependencies are listed in `requirements.txt`.

Java is also required for Spark execution inside the Airflow Docker container.

---

# Expected Results

After a successful batch run, the following folders should be created:

```text
data/landing/
data/bronze/
data/silver/
data/gold/avg_stats
```

The final Gold table should contain aggregated statistics:

```text
sport
medal
sex
country_noc
avg_weight
avg_height
timestamp
```

Each Spark job prints its final DataFrame using `df.show()`. These outputs are available in Airflow task logs.

---

# Screenshots for Submission

The final submission should include:

- screenshot of the successful Airflow DAG run;
- screenshots of task logs showing `df.show()` output;
- screenshot or evidence of the final Gold table output;
- code files for both project parts.

---

# Author

Oleksandr Novokhatskyi
