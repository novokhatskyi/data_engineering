import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, trim, regexp_replace
from pyspark.sql.types import DoubleType


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

os.makedirs(GOLD_DIR, exist_ok=True)

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

athlete_bio = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_bio"))
athlete_event_results = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_event_results"))

athlete_bio_selected = athlete_bio.select(
    "athlete_id",
    "sex",
    "country_noc",
    "height",
    "weight"
).distinct() \
    .where(trim(col("height")) != "") \
    .where(trim(col("weight")) != "") \
    .withColumn("weight", regexp_replace(col("weight"), ",", ".")) \
    .withColumn("height", col("height").cast(DoubleType())) \
    .withColumn("weight", col("weight").cast(DoubleType())) \
    .dropna(subset=["height", "weight"]) \
    .filter((col("height") >= 100) & (col("height") <= 250)) \
    .filter((col("weight") >= 25) & (col("weight") <= 250))

athlete_event_results_selected = athlete_event_results.select(
    "athlete_id",
    "sport",
    "medal"
)

joined = athlete_event_results_selected.join(
    athlete_bio_selected,
    on="athlete_id",
    how="inner"
)

gold_df = joined.groupBy(
    "sport",
    "medal",
    "sex",
    "country_noc"
).agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn(
    "timestamp",
    current_timestamp()
)

gold_path = os.path.join(GOLD_DIR, "avg_stats")
gold_df.write.mode("overwrite").parquet(gold_path)
print(f"Saved gold table: {gold_path}")
print("Final DataFrame for gold/avg_stats:")
gold_df.show(20, truncate=False)

spark.stop()