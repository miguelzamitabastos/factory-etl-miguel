import threading
import os
from utils.kafka_client import create_raw_consumer, send_rich_event
from utils.elasticsearch import save_raw_event, save_rich_event
from pipeline.stream import kafka_observable, build_pipeline
from rx import operators as ops

def print_error(e):
    print(f"stream error: {e}")

def main():
    consumer = create_raw_consumer()

    source = kafka_observable(consumer)

    pipeline = build_pipeline(source,send_rich_event,save_raw_event,save_rich_event)

    pipeline.subscribe(
        on_error=print_error
    )

    threading.Event().wait()


if __name__ == "__main__":
    main()