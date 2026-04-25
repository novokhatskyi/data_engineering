from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
from pyspark.sql.types import DoubleType
from configs import (JDBC_PASSWORD, JDBC_URL, JDBC_USER, MYSQL_JAR_PATH, 
                     ATHLETE_BIO_TABLE
)

# Створення Spark сесії
def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars", MYSQL_JAR_PATH)\
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")\
        .config("spark.sql.shuffle.partitions", "2")\
        .appName("FinalStreamingProject")\
        .getOrCreate()
    return spark

# Читання даних з SQL бази даних
def load_and_clean_bio_table(spark):
    df_bio_table = spark.read.format("jdbc").options(
        url=JDBC_URL,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=ATHLETE_BIO_TABLE,
        user=JDBC_USER,
        password=JDBC_PASSWORD
    ).load()

    df_bio_table = df_bio_table.select("*") \
        .distinct() \
        .where(trim(col("height")) != "")\
        .where(trim(col("weight")) != "")\
        .withColumn("weight", regexp_replace(col("weight"), ",", "."))\
        .withColumn("height", col("height").cast(DoubleType()))\
        .withColumn("weight", col("weight").cast(DoubleType()))\
        .dropna(subset=["height", "weight"])\
        .filter((col("height") >= 100) & (col("height") <= 250))\
        .filter((col("weight") >= 25) & (col("weight") <= 250))\
        .dropna(subset=["height", "weight"])
    return df_bio_table
