import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())
spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    bronze_path = os.path.join(BRONZE_DIR, table)
    silver_path = os.path.join(SILVER_DIR, table)
    df = spark.read.parquet(bronze_path)
    for column in df.schema.fields:
        if isinstance(column.dataType, StringType):
            df = df.withColumn(column.name, clean_text_udf(df[column.name]))
    df = df.dropDuplicates()
    df.write.mode("overwrite").parquet(silver_path)
    print(f"Saved silver table: {silver_path}")
    print(f"Final DataFrame for silver/{table}:")
    df.show(20, truncate=False)
spark.stop()