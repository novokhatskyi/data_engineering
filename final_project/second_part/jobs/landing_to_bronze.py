import os
import requests
from pyspark.sql import SparkSession


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LANDING_DIR = os.path.join(BASE_DIR, "data", "landing")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

os.makedirs(LANDING_DIR, exist_ok=True)
os.makedirs(BRONZE_DIR, exist_ok=True)


def download_data(table_name):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + table_name + ".csv"

    local_file_path = os.path.join(LANDING_DIR, table_name + ".csv")

    print(f"Downloading from {downloading_url}")
    print(f"Saving to {local_file_path}")

    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully: {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")
    return local_file_path

spark = SparkSession.builder.appName("landing_to_bronze").getOrCreate()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    csv_path = download_data(table)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)
    bronze_path = os.path.join(BRONZE_DIR, table)
    df.write.mode("overwrite").parquet(bronze_path)
    print(f"Saved bronze table: {bronze_path}")
    print(f"Final DataFrame for bronze/{table}:")
    df.show(20, truncate=False)

spark.stop()