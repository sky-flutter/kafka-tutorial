from sseclient import SSEClient
from kafka import KafkaProducer


class SSEClientHandler(SSEClient):

    def __init__(self, event_source, producer: KafkaProducer, topic: str, char_enc='utf-8'):
        print("Started event source")
        self.producer = producer
        self.topic = topic
        super().__init__(event_source, char_enc)

    def listen_change(self):
        for event in self.events():
            self.producer.send(self.topic, value=event.data)
