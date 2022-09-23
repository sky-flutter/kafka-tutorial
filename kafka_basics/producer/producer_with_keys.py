import logging
from base.producer import BaseProducer
from utils.constants import Constant


logging.basicConfig(level=logging.DEBUG)

'''
Class helps to send data to the kafka with keys and values
'''


class ProducerWithKeys(BaseProducer):

    '''
    Send message to the kafka server
    '''

    def send_message_with_key(self, key, message):
        self.producer.send(Constant.TOPIC_NAME, value=message, key=key)
        logging.info("Message sent")

    def __close__(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer_demo = ProducerWithKeys()
    for i in range(10):
        producer_demo.send_message_with_key((f"id_{i}").encode(), b'Hello, Kafka')
    # producer_demo.__close__()
