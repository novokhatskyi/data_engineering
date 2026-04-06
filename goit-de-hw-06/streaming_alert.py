from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from configs import kafka_config, ALERT_TOPIC, ALERT_CSV_FILE, RAW_TOPIC
import os
import pyspark

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f'--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__} pyspark-shell'
)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("IoT Streaming Alert") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

alert_conditions_df = spark.read.csv(ALERT_CSV_FILE, header=True, inferSchema=True)

kafka_options = {
    "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
    "kafka.security.protocol": kafka_config["security_protocol"],
    "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
    "kafka.sasl.jaas.config":
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";'
}

stream_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", RAW_TOPIC) \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

aggregated_df = stream_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

alerts = aggregated_df.crossJoin(alert_conditions_df) \
    .filter(
        ((col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min"))) &
        ((col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max"))) &
        ((col("temperature_min") == -999) | (col("avg_temperature") >= col("temperature_min"))) &
        ((col("temperature_max") == -999) | (col("avg_temperature") <= col("temperature_max")))
    ) \
    .select(
        col("window"),
        col("avg_temperature").alias("t_avg"),
        col("avg_humidity").alias("h_avg"),
        col("code").cast("string"),
        col("message"),
        current_timestamp().alias("timestamp")
    )

def write_to_kafka(df, epoch_id):
    print(f"Epoch {epoch_id}: {df.count()} alerts")

    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .options(**kafka_options) \
        .option("topic", ALERT_TOPIC) \
        .save()

query = alerts.writeStream \
    .foreachBatch(write_to_kafka) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/streaming_alert") \
    .start()

query.awaitTermination()