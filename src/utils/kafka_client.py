import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
import kafka.errors

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
raw_topic = os.getenv("KAFKA_RAW_TOPIC")
rich_topic = os.getenv("KAFKA_RICH_TOPIC")

logger = logging.getLogger(__name__)

def _create_with_retry(factory_func, max_retries=10, delay=5):
    retries = 0
    while retries < max_retries:
        try:
            return factory_func()
        except kafka.errors.NoBrokersAvailable:
            retries += 1
            logger.warning(f"Kafka brokers not available. Retrying {retries}/{max_retries} in {delay}s...")
            time.sleep(delay)
    raise kafka.errors.NoBrokersAvailable(f"Failed to connect to Kafka after {max_retries} retries.")

def create_raw_consumer():
    def factory():
        return KafkaConsumer(
            raw_topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True
        )
    return _create_with_retry(factory)

def create_producer():
    def factory():
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    return _create_with_retry(factory)

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = create_producer()
    return _producer

def send_rich_event(event):
    producer = get_producer()
    producer.send(
            topic=rich_topic,
            value=event
        )
    producer.flush()
