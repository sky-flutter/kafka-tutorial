import requests
import json
import logging
from sse_client_handler import SSEClientHandler
from kafka import KafkaProducer


# logging.basicConfig(level=logging.DEBUG)

class WikimediaChangeProducer:
    topic = 'wikimedia.recentchange'
    recent_change_api = "https://stream.wikimedia.org/v2/stream/recentchange"

    def __init__(self):
        self.create_producer()

    def create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9091", value_serializer=lambda v: json.dumps(v).encode('utf-8'), compression_type="snappy", batch_size=32*1024, linger_ms=20)

    def listen_for_event(self):
        stream_response = requests.get(self.recent_change_api, stream=True)
        client = SSEClientHandler(
            stream_response, producer=self.producer, topic=self.topic)
        client.listen_change()


if __name__ == "__main__":
    producer = WikimediaChangeProducer()
    producer.listen_for_event()
