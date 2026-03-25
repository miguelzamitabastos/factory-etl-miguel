import os
import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
raw_topic = os.getenv("KAFKA_RAW_TOPIC")
rich_topic = os.getenv("KAFKA_RICH_TOPIC")


def create_raw_consumer():
    return KafkaConsumer(
        raw_topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

producer = create_producer()

def send_rich_event(event):
    producer.send(
            topic=rich_topic,
            value=event
        )
    producer.flush()
