from kafka import KafkaProducer
import json

KAFKA_SERVER = "kafka:9092"
TOPIC = "bank-transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish_event(event: dict):
    producer.send(TOPIC, event)
    producer.flush()
