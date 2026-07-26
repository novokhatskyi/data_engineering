from kafka import KafkaProducer
from configs import kafka_config, RAW_TOPIC
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

try:
    while True:
    # Відправлення повідомлення в топік
        try:
            data = {
                "timestamp": time.time(),  # Часова мітка
                "sensor_id": str(uuid.uuid4()),  # Унікальний ідентифікатор датчика
                "temperature": random.uniform(70, 90),  # Температура
                "humidity": random.uniform(85, 100)  # Вологість
            }
            producer.send(topic=RAW_TOPIC, value=data)
            print(f"Message sent to topic '{RAW_TOPIC}': {data}")
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(2)  # Затримка між повідомленнями

except KeyboardInterrupt:
    print("Producer stopped by user.")

# Закриття зв'язку з клієнтом
producer.close()