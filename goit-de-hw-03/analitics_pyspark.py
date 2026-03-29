from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

from rich import print
from rich.console import Console
from rich.table import Table


# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyHWSpark") \
    .getOrCreate()

# read data
df_users = spark.read.csv("data/users.csv", header=True, inferSchema=True)
df_purchases = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)   
df_products = spark.read.csv("data/products.csv", header=True, inferSchema=True) 

df_users.describe().show()
df_users.printSchema()

df_purchases.describe().show()
df_purchases.printSchema()

df_purchases.describe().show()
df_products.printSchema()

# Clean data
df_users_clean = df_users.select("*")\
    .where(col("user_id").isNotNull())\
    .where(col("name").isNotNull())\
    .where(col("email").isNotNull())\
    .where(col("age").isNotNull())

df_purchases_clean = df_purchases.select("*")\
    .where(col("purchase_id").isNotNull())\
    .where(col("user_id").isNotNull())\
    .where(col("product_id").isNotNull())\
    .where(col("quantity").isNotNull())\
    .where(col("date").isNotNull())


df_products_clean = df_products.select("*")\
    .where(col("product_id").isNotNull())\
    .where(col("product_name").isNotNull())\
    .where(col("category").isNotNull())\
    .where(col("price").isNotNull())

df_users_clean.describe().show()
df_purchases_clean.describe().show()
df_products_clean.describe().show()
print("\n[bold blue]2. Total records before and after cleaning:[bold blue]")
print("\n[bold blue]Users:[bold blue] [bold cyan]before =[bold cyan]", df_users.count(), "[bold cyan]after clean =[bold cyan]", df_users_clean.count())
print("[bold blue]Purchases:[bold blue] [bold cyan]before =[bold cyan]", df_purchases.count(), "[bold cyan]after clean =[bold cyan]", df_purchases_clean.count())
print("[bold blue]Products:[bold blue] [bold cyan]before =[bold cyan]", df_products.count(), "[bold cyan]after clean =[bold cyan]", df_products_clean.count())


# Total spent on each product category.
df_joined = df_purchases_clean.join(df_products_clean, "product_id", "inner")
df_joined = df_joined.withColumn("total_spent", col("quantity") * col("price"))
total_spent_by_category = df_joined.groupBy("category")\
    .sum("total_spent")\
    .withColumnRenamed("sum(total_spent)", "total_spent")\
    .withColumn("total_spent", round(col("total_spent"), 2))

print("\n[bold blue]3. Total spent on each product category:[bold blue]")
total_spent_by_category.show(5)

# Total spent on each product category by users aged 18-25.
df_full = df_users_clean.join(df_joined, "user_id", "inner")
df_full_18_25 = df_full.filter((col("age") >= 18) & (col("age") <= 25))
total_spent_18_25 = df_full_18_25.groupBy("category").sum("total_spent")\
    .withColumnRenamed("sum(total_spent)", "total_spent_18_25")\
    .withColumn("total_spent_18_25", round(col("total_spent_18_25"), 2))

print("\n[bold blue]4. Total spent on each product category by users aged 18-25:[bold blue]")
total_spent_18_25.show(10)

# Percentage of total spending by users aged 18-25 in each product category.
percentage_spent_18_25 = total_spent_by_category.join(total_spent_18_25, "category", "left")\
    .withColumn("percentage_spent_18_25", \
                round((col("total_spent_18_25") / col("total_spent")) * 100, 2))
print("\n[bold blue]5. Percentage of total spending by users aged 18-25 in each product category:[bold blue]")
percentage_spent_18_25.show(10)

sorted_percentage = percentage_spent_18_25.orderBy(col("percentage_spent_18_25").desc()).limit(3)
print("\n[bold blue]6. Top 3 categories with highest spending by users aged 18-25:[bold blue]")
sorted_percentage.show(10)
