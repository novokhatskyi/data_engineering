from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
from rich import print

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)
my_name = "oleksandr"
topic_name_1 = f'{my_name}_building_sensors'
topic_name_2 = f'{my_name}_temperature_alerts'
topic_name_3 = f'{my_name}_humidity_alerts'
num_partitions = 2
replication_factor = 1


topic_building_sensors = NewTopic(name=topic_name_1, 
                     num_partitions=num_partitions, 
                     replication_factor=replication_factor
            )

topic_temperature_alerts = NewTopic(name=topic_name_2, 
                     num_partitions=num_partitions, 
                     replication_factor=replication_factor
            )

topic_humidity_alerts = NewTopic(name=topic_name_3, 
                     num_partitions=num_partitions, 
                     replication_factor=replication_factor
            )

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[topic_building_sensors, 
                                           topic_temperature_alerts, 
                                           topic_humidity_alerts], 
                                           validate_only=False)
    print(f"Topics created successfully.")
except Exception as e:
    print(f"\n[bold red]These topics already exist, so I'm skipping creating them:[/bold red] {e}\n")

# Перевіряємо список існуючих топіків 
print(admin_client.list_topics())

# Закриття зв'язку з клієнтом
admin_client.close()
