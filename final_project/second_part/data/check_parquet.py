import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("check_parquet").getOrCreate()

bio_path = os.path.join(BASE_DIR, "silver", "athlete_bio")
results_path = os.path.join(BASE_DIR, "silver", "athlete_event_results")

df_bio = spark.read.parquet(bio_path)
df_results = spark.read.parquet(results_path)

print("=== athlete_bio schema ===")
df_bio.printSchema()

print("=== athlete_event_results schema ===")
df_results.printSchema()

spark.stop()