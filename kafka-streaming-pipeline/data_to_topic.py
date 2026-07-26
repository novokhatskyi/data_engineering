from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
from rich import print

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

my_name = "oleksandr"
topic_name_1 = f'{my_name}_building_sensors'


for i in range(10):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "sensor_id": str(uuid.uuid4()),  # Унікальний ідентифікатор датчика
            "temperature": random.uniform(25, 45),  # Температура
            "humidity": random.uniform(15, 85)  # Вологість
        }
        producer.send(topic=topic_name_1, value=data)
        print(f"Message sent to topic '{topic_name_1}': {data}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Закриття зв'язку з клієнтом
producer.close()