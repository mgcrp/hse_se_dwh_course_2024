import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_HOST = "localhost"
KAFKA_TOPIC_NAME = "service_backend_feed"

class MessageProducer:
    broker = None
    topic = None
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries = 3
        )

    def send_msg(self, msg):
        future = self.producer.send(self.topic, msg)
        self.producer.flush()

kafka_client = MessageProducer(
    f"{KAFKA_HOST}:9092",
    KAFKA_TOPIC_NAME
)

kafka_client.send_msg({
    "created_at": datetime.utcnow().isoformat(),
    "model_name": "__model",
    "method": "__method",
    "image_input": "__uuid.__fileExt",
    "image_output": "__output"
})