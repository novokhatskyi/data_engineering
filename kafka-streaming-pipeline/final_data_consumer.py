from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
from rich import print

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

my_name = "oleksandr"
topic_name_2 = f'{my_name}_temperature_alerts'
topic_name_3 = f'{my_name}_humidity_alerts'

# Підписка на тему
consumer.subscribe([topic_name_2, topic_name_3])
print(f"Subscribed to topics '{topic_name_2}' and '{topic_name_3}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        if message.topic == topic_name_2:
            print(f"\n[bold red]Temperature alert received from topic[/bold red] '{topic_name_2}': {message.value}\n")
        if message.topic == topic_name_3:
            print(f"\n[bold blue]Humidity alert received from topic[/bold blue] '{topic_name_3}': {message.value}\n")
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    consumer.close() 