from bio_cleaning import create_spark_session, load_and_clean_bio_table
from kafka_pipeline import load_results_table, create_kafka_topics, data_to_kafka, write_results_to_kafka, read_from_kafka
from stream_processing import join_events_with_bio, normalize_medal, aggregate_athlete_stats
from output import foreach_batch_function
from configs import CHECKPOINT_LOCATION

spark = create_spark_session()
print("Spark version:", spark.version)

create_kafka_topics()

df_bio_table = load_and_clean_bio_table(spark)
df_results_table = load_results_table(spark)

df_bio_table.printSchema()
df_bio_table.describe(["height", "weight"]).show()
# print("Після очищення:", df_bio_table.count())

df_results_table.printSchema()
df_results_table.show(5, truncate=False)

# df_for_kafka = data_to_kafka(df_results_table)
# df_for_kafka.show(5, truncate=False)
# write_results_to_kafka(df_for_kafka)

stream_events_df = read_from_kafka(spark)
joined_df = join_events_with_bio(stream_events_df, df_bio_table)
normalized_df = normalize_medal(joined_df)
aggregated_df = aggregate_athlete_stats(normalized_df)
aggregated_df.printSchema()

query = aggregated_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

df_for_kafka = data_to_kafka(df_results_table.limit(1000))
write_results_to_kafka(df_for_kafka)

query.awaitTermination(120)
query.stop()