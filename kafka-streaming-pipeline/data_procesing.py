from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
from rich import print

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
topic_name_1 = f'{my_name}_building_sensors'
topic_name_2 = f'{my_name}_temperature_alerts'
topic_name_3 = f'{my_name}_humidity_alerts'

# Підписка на тему
consumer.subscribe([topic_name_1])
print(f"Subscribed to topic '{topic_name_1}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        if message.value['temperature'] > 40:
            alert_data = {
                "timestamp": time.time(),
                "sensor_id": message.value['sensor_id'],
                "temperature": message.value['temperature']
            }
            producer.send(topic=topic_name_2, value=alert_data)
            print(f"\n[bold yellow]Temperature alert sent to topic[/bold yellow]'{topic_name_2}': {alert_data}\n")
        if message.value['humidity'] > 80 or message.value['humidity'] < 20:
            alert_data = {
                "timestamp": time.time(),
                "sensor_id": message.value['sensor_id'],
                "humidity": message.value['humidity']
            }
            producer.send(topic=topic_name_3, value=alert_data)
            print(f"\n[bold yellow]Humidity alert sent to topic[/bold yellow]'{topic_name_3}': {alert_data}\n")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close() 
    producer.close()