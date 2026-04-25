from pyspark.sql.functions import col, when, trim, lower, avg, round, current_timestamp


def join_events_with_bio(stream_events_df, df_bio_table):
    joined_df = stream_events_df.join(
        df_bio_table,
        stream_events_df.athlete_id == df_bio_table.athlete_id,
        how="inner"
    ).select(
        stream_events_df["*"],
        df_bio_table["height"],
        df_bio_table["weight"],
        df_bio_table["sex"],
        df_bio_table["country_noc"]
    )

    return joined_df

def normalize_medal(joined_df):
    normalized_df = joined_df.withColumn(
        "medal_type",
        when(
            col("medal").isNull() |
            (trim(lower(col("medal"))) == "nan") |
            (trim(col("medal")) == ""),
            "No medal"
        ).otherwise(col("medal"))
    )

    return normalized_df

def aggregate_athlete_stats(normalized_df):
    aggregated_df = normalized_df.groupBy(
        "sport",
        "medal_type",
        "sex",
        "country_noc"
    ).agg(
        round(avg("height"), 2).alias("avg_height"),
        round(avg("weight"), 2).alias("avg_weight")
    ).withColumn(
        "calculated_at",
        current_timestamp()
    )

    return aggregated_df


