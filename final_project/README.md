# Final Project — Data Engineering

## Опис проєкту

Цей репозиторій містить фінальний проєкт з Data Engineering, який складається з двох частин:

1. **Streaming pipeline** — обробка потокових даних з Kafka за допомогою Spark Structured Streaming.
2. **Batch Data Lake** — побудова простого multi-hop Data Lake з рівнями `landing`, `bronze`, `silver`, `gold` за допомогою Spark та Airflow.

---

# Частина 1. Streaming pipeline

## Суть завдання

У першій частині фінального проєкту потрібно побудувати streaming-рішення для букмекерської компанії.

Фізичні дані атлетів зберігаються заздалегідь у MySQL-таблиці:

```text
olympic_dataset.athlete_bio
```

Результати спортивних подій надходять у Kafka-топік у режимі потоку.

Завдання полягає в тому, щоб:

1. Зчитати дані атлетів з MySQL.
2. Зчитати результати подій з Kafka.
3. Об'єднати ці дані за `athlete_id`.
4. Очистити та підготувати числові поля `height` і `weight`.
5. Згрупувати дані за необхідними бізнес-ознаками.
6. Розрахувати середні значення зросту та ваги.
7. Записати результат у вихідний Kafka-топік та/або базу даних.

## Основна логіка

Пайплайн виконує такі кроки:

```text
MySQL athlete_bio
        +
Kafka athlete_event_results
        ↓
Spark Structured Streaming
        ↓
clean height / weight
        ↓
join by athlete_id
        ↓
aggregation
        ↓
output Kafka / database
```

## Очищення даних

У першій частині була використана логіка очищення таблиці `athlete_bio`:

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
    .filter((col("weight") >= 25) & (col("weight") <= 250)) \
    .dropna(subset=["height", "weight"])
```

Ця логіка потрібна, тому що в колонці `weight` можуть бути значення з комою, наприклад:

```text
100,104
```

Перед приведенням до `DoubleType` кома замінюється на крапку.

---

# Частина 2. Batch Data Lake

## Суть завдання

У другій частині фінального проєкту потрібно побудувати batch Data Lake з трирівневою архітектурою обробки даних.

Дані беруться з FTP/HTTPS-сервера GoIT:

```text
https://ftp.goit.study/neoversity/athlete_bio.csv
https://ftp.goit.study/neoversity/athlete_event_results.csv
```

Потрібно реалізувати обробку даних через такі рівні:

```text
landing → bronze → silver → gold
```

## Архітектура

```text
CSV files from GoIT FTP
        ↓
landing layer
        ↓
bronze layer: parquet
        ↓
silver layer: cleaned parquet
        ↓
gold layer: analytical dataset
        ↓
Airflow DAG
```

## Структура проєкту

```text
second_part/
├── dags/
│   └── project_solution.py
│
├── jobs/
│   ├── landing_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── data/
│   ├── landing/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Файл `landing_to_bronze.py`

### Призначення

Файл завантажує CSV-файли з GoIT FTP/HTTPS-сервера, читає їх за допомогою Spark і зберігає у форматі Parquet у bronze layer.

### Вхідні дані

```text
https://ftp.goit.study/neoversity/athlete_bio.csv
https://ftp.goit.study/neoversity/athlete_event_results.csv
```

### Вихідні дані

```text
data/landing/athlete_bio.csv
data/landing/athlete_event_results.csv

data/bronze/athlete_bio
data/bronze/athlete_event_results
```

### Основні кроки

```text
download CSV
↓
read CSV with Spark
↓
write Parquet to bronze/{table}
↓
show final DataFrame in Airflow logs
```

---

## Файл `bronze_to_silver.py`

### Призначення

Файл читає дані з bronze layer, очищує всі текстові колонки, видаляє дублікати та записує результат у silver layer.

### Вхідні дані

```text
data/bronze/athlete_bio
data/bronze/athlete_event_results
```

### Вихідні дані

```text
data/silver/athlete_bio
data/silver/athlete_event_results
```

### Очищення тексту

Для очищення текстових колонок використовується функція:

```python
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))
```

Функція обгортається у Spark UDF:

```python
clean_text_udf = udf(clean_text, StringType())
```

Потім застосовується до всіх колонок типу `StringType`.

### Основні кроки

```text
read bronze parquet
↓
clean string columns
↓
drop duplicates
↓
write Parquet to silver/{table}
↓
show final DataFrame in Airflow logs
```

---

## Файл `silver_to_gold.py`

### Призначення

Файл читає дві silver-таблиці, об'єднує їх за `athlete_id`, рахує середні значення `weight` і `height` для кожної комбінації:

```text
sport
medal
sex
country_noc
```

Потім додає колонку `timestamp` і записує фінальну таблицю в gold layer.

### Вхідні дані

```text
data/silver/athlete_bio
data/silver/athlete_event_results
```

### Вихідні дані

```text
data/gold/avg_stats
```

### Фінальні колонки

```text
sport
medal
sex
country_noc
avg_weight
avg_height
timestamp
```

### Основні кроки

```text
read silver tables
↓
clean and cast height / weight
↓
join by athlete_id
↓
group by sport, medal, sex, country_noc
↓
calculate avg_weight and avg_height
↓
add timestamp
↓
write Parquet to gold/avg_stats
↓
show final DataFrame in Airflow logs
```

---

## Файл `project_solution.py`

### Призначення

Файл містить Airflow DAG, який послідовно запускає три Spark jobs:

```text
landing_to_bronze
        ↓
bronze_to_silver
        ↓
silver_to_gold
```

### DAG

```text
final_project_batch_datalake
```

### Airflow tasks

```text
landing_to_bronze
bronze_to_silver
silver_to_gold
```

### Оператор

Для запуску Spark jobs використовується:

```python
SparkSubmitOperator
```

Шляхи до jobs всередині Docker-контейнера:

```text
/opt/airflow/jobs/landing_to_bronze.py
/opt/airflow/jobs/bronze_to_silver.py
/opt/airflow/jobs/silver_to_gold.py
```

---

# Локальний запуск Spark jobs без Airflow

Перед запуском через Airflow кожен Spark job був протестований локально.

З кореня `second_part`:

```bash
python jobs/landing_to_bronze.py
python jobs/bronze_to_silver.py
python jobs/silver_to_gold.py
```

Після успішного запуску мають бути створені папки:

```text
data/landing
data/bronze
data/silver
data/gold
```

---

# Запуск через Airflow

## 1. Підготовка Docker

Airflow запускається через Docker Compose.

У `docker-compose.yaml` мають бути підключені локальні папки:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/jobs:/opt/airflow/jobs
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
```

## 2. Java для Spark

Для роботи `spark-submit` у контейнері потрібна Java.

У проєкті використовується власний `Dockerfile`, який встановлює Java та потрібні Python-залежності.

## 3. Запуск Airflow

```bash
docker compose build
docker compose up -d
```

Airflow UI доступний за адресою:

```text
http://localhost:8080
```

Логін і пароль за замовчуванням:

```text
airflow / airflow
```

## 4. Spark connection

В Airflow потрібно створити connection:

```text
Connection Id: spark-default
Connection Type: Spark
Host: local[*]
```

Через CLI:

```bash
docker compose exec airflow-apiserver airflow connections add spark-default \
  --conn-type spark \
  --conn-host 'local[*]'
```

Перевірка:

```bash
docker compose exec airflow-apiserver airflow connections get spark-default
```

## 5. Запуск DAG

У Airflow UI потрібно знайти DAG:

```text
final_project_batch_datalake
```

Після запуску задачі мають виконатися в такому порядку:

```text
landing_to_bronze → bronze_to_silver → silver_to_gold
```

---

# Результат

Після успішного виконання DAG створюється фінальна gold-таблиця:

```text
data/gold/avg_stats
```

Вона містить середні значення ваги та зросту спортсменів для кожної комбінації:

```text
sport
medal
sex
country_noc
```

Приклад фінального результату:

```text
+-------------------+------+----+-----------+------------------+------------------+--------------------------+
|sport              |medal |sex |country_noc|avg_weight        |avg_height        |timestamp                 |
+-------------------+------+----+-----------+------------------+------------------+--------------------------+
|Swimming           |None  |Male|DEN        |82.20833333333333 |189.67708333333334|2026-04-26 20:14:48.318945|
|Athletics          |None  |Male|GBS        |62.0              |164.2             |2026-04-26 20:14:48.318945|
|Volleyball         |None  |Female|PER      |65.4375           |173.25            |2026-04-26 20:14:48.318945|
+-------------------+------+----+-----------+------------------+------------------+--------------------------+
```

---

# Скриншоти для здачі

Для здачі проєкту потрібно додати скриншоти:

1. Логи `landing_to_bronze` з результатом `df.show()`.
2. Логи `bronze_to_silver` з результатом `df.show()`.
3. Логи `silver_to_gold` з результатом `gold_df.show()`.
4. Граф Airflow DAG, де всі задачі мають статус `Success`.

---

# Використані технології

```text
Python
Apache Spark
PySpark
Apache Airflow
SparkSubmitOperator
Docker
Docker Compose
Parquet
CSV
Kafka
MySQL
```

---

# Автор

Oleksandr Novokhatskyi
