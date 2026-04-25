from bio_cleaning import create_spark_session, load_and_clean_bio_table
from kafka_pipeline import load_results_table, create_kafka_topics, data_to_kafka

spark = create_spark_session()

create_kafka_topics()

df_bio_table = load_and_clean_bio_table(spark)
df_results_table = load_results_table(spark)

df_bio_table.printSchema()
df_bio_table.describe(["height", "weight"]).show()
# print("Після очищення:", df_bio_table.count())

df_results_table.printSchema()
df_results_table.show(5, truncate=False)

df_for_kafka = data_to_kafka(df_results_table)
df_for_kafka.show(5, truncate=False)

