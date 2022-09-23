import logging
from utils.constants import Constant
from base.producer import BaseProducer

logging.basicConfig(level=logging.DEBUG)

'''
Class helps to send data to the kafka
'''
class ProducerDemo(BaseProducer):

    '''
    Send message to the kafka server
    '''
    def send_message(self, message):
        self.producer.send(Constant.TOPIC_NAME, message)
        logging.info("Message sent")

    def __close__(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer_demo = ProducerDemo()
    producer_demo.send_message(b'Test, Kafka')
    # producer_demo.__close__()