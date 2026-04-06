from kafka import KafkaConsumer
from configs import kafka_config, ALERT_TOPIC
import json
from rich import print

consumer = KafkaConsumer(
    ALERT_TOPIC,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening topic: {ALERT_TOPIC}")

for message in consumer:
    print(message.value)