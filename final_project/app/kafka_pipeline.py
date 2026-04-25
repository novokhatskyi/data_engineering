from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql.functions import col, to_json, struct
from rich import print
from configs import (JDBC_PASSWORD, JDBC_URL, 
                     JDBC_USER, ATHLETE_RESULTS_TABLE, 
                     kafka_config, KAFKA_INPUT_TOPIC, KAFKA_OUTPUT_TOPIC
)

def load_results_table(spark):
    df_results_table = spark.read.format("jdbc").options(
        url=JDBC_URL,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=ATHLETE_RESULTS_TABLE,
        user=JDBC_USER,
        password=JDBC_PASSWORD
    ).load()
    return df_results_table

def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )

    num_partitions = 2
    replication_factor = 1

    topic_1 = NewTopic(name=KAFKA_INPUT_TOPIC, 
                        num_partitions=num_partitions, 
                        replication_factor=replication_factor
                )
    topic_2 = NewTopic(name=KAFKA_OUTPUT_TOPIC, 
                        num_partitions=num_partitions, 
                        replication_factor=replication_factor
                )
    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[topic_1, 
                                            topic_2], 
                                            validate_only=False)
        print(f"Topics created successfully.")
    except Exception as e:
        print(f"\n[bold red]These topics already exist, so I'm skipping creating them:[/bold red] {e}\n")

    # Перевіряємо список існуючих топіків 
    print(admin_client.list_topics())

    # Закриття зв'язку з клієнтом
    admin_client.close()

def data_to_kafka(df_results_table):
    df_for_kafka = df_results_table.select(
        to_json(
            struct(
                col("edition"),
                col("athlete_id"),
                col("sport"),
                col("event"),
                col("medal")
        )
    ).alias("value")
    )
    return df_for_kafka

