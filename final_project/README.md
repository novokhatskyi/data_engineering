# Final Project: Batch Data Lake with Spark and Airflow

## Project Overview

This project implements a simple multi-hop batch Data Lake pipeline using Apache Spark and Apache Airflow.

The pipeline processes Olympic athlete data and event results from CSV source files, transforms them through several data lake layers, and produces a final analytical dataset with average athlete statistics.

The project follows a classic multi-layer data architecture:

```text
Landing → Bronze → Silver → Gold
```

## Data Sources

The project uses two source datasets provided by GoIT:

- `athlete_bio.csv`
- `athlete_event_results.csv`

Source URLs:

```text
https://ftp.goit.study/neoversity/athlete_bio.csv
https://ftp.goit.study/neoversity/athlete_event_results.csv
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

## Pipeline Description

### 1. Landing to Bronze

File:

```text
jobs/landing_to_bronze.py
```

This Spark job:

- downloads CSV files from the source URLs;
- saves raw CSV files into the landing layer;
- reads the CSV files using Spark;
- writes the data in Parquet format into the bronze layer;
- prints the final DataFrame with `df.show()` for Airflow task logs.

Output:

```text
data/landing/athlete_bio.csv
data/landing/athlete_event_results.csv
data/bronze/athlete_bio
data/bronze/athlete_event_results
```

### 2. Bronze to Silver

File:

```text
jobs/bronze_to_silver.py
```

This Spark job:

- reads Parquet data from the bronze layer;
- applies text cleaning to all string columns;
- removes duplicate rows;
- writes cleaned data into the silver layer;
- prints the final DataFrame with `df.show()` for Airflow task logs.

Output:

```text
data/silver/athlete_bio
data/silver/athlete_event_results
```

### 3. Silver to Gold

File:

```text
jobs/silver_to_gold.py
```

This Spark job:

- reads both silver tables;
- joins them by `athlete_id`;
- cleans and converts `height` and `weight` to numeric types;
- groups data by:
  - `sport`
  - `medal`
  - `sex`
  - `country_noc`
- calculates average `weight` and `height`;
- adds a processing timestamp;
- writes the final analytical dataset into the gold layer;
- prints the final DataFrame with `df.show()` for Airflow task logs.

Output:

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

## Airflow DAG

File:

```text
dags/project_solution.py
```

The Airflow DAG runs all Spark jobs sequentially:

```text
landing_to_bronze → bronze_to_silver → silver_to_gold
```

The DAG uses `SparkSubmitOperator` to execute Spark jobs.

## Running the Project Locally

### 1. Build and start Airflow

```bash
docker compose build
docker compose up -d
```

### 2. Open Airflow UI

```text
http://localhost:8080
```

Default credentials:

```text
Username: airflow
Password: airflow
```

### 3. Configure Spark connection

Create an Airflow connection:

```text
Connection Id: spark-default
Connection Type: Spark
Host: local[*]
```

This allows `SparkSubmitOperator` to run Spark jobs locally inside the Airflow environment.

### 4. Run the DAG

Find the DAG:

```text
final_project_batch_datalake
```

Trigger it manually from the Airflow UI.

## Requirements

Main dependencies:

```text
apache-airflow-providers-apache-spark
pyspark
requests
```

These dependencies are also listed in `requirements.txt`.

## Expected Result

After successful execution, the following data layers should be created:

```text
data/landing/
data/bronze/
data/silver/
data/gold/avg_stats
```

Each Spark job prints its final DataFrame using `df.show()` so that the results can be reviewed in Airflow task logs.

## Screenshots for Submission

The project submission should include:

- screenshot of successful Airflow DAG execution;
- screenshots of task logs with `df.show()` output;
- screenshot or evidence of the final gold table output.

## Author

Oleksandr Novokhatskyi
