import json
from kafka import KafkaConsumer

KAFKA_HOST = "localhost"
KAFKA_TOPIC_NAME = "service_backend_feed"
KAFKA_CONSUMER_GROUP = "backend"

consumer = KafkaConsumer(
    bootstrap_servers=f"{KAFKA_HOST}:9092",
    value_deserializer=lambda v: json.loads( v.decode("utf-8") ),
    auto_offset_reset="earliest",
    group_id=KAFKA_CONSUMER_GROUP
)
consumer.subscribe(topics=KAFKA_TOPIC_NAME)
print('>>> Consumer started')

try:
    for message in consumer:
        value = message.value
        print(value)
except Exception as e:
        print("Closing consumer due to error")
        consumer.close()
        raise e
finally:
    print("Closing consumer due to finish")
    consumer.close()