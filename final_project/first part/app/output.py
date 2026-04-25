from pyspark.sql.functions import to_json, struct, col
from configs import (
    kafka_config,
    KAFKA_OUTPUT_TOPIC,
    JDBC_URL,
    JDBC_USER,
    JDBC_PASSWORD,
    OUTPUT_TABLE,
)


def foreach_batch_function(batch_df, batch_id):
    kafka_df = batch_df.select(
        to_json(
            struct(
                col("sport"),
                col("medal_type"),
                col("sex"),
                col("country_noc"),
                col("avg_height"),
                col("avg_weight"),
                col("calculated_at")
            )
        ).alias("value")
    )

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        ) \
        .option("topic", KAFKA_OUTPUT_TOPIC) \
        .save()

    batch_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", OUTPUT_TABLE) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .mode("append") \
        .save()