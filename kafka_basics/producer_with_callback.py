import logging
from base.producer import BaseProducer
from utils.constants import Constant
from utils.logger import Logger
logging.basicConfig(level=logging.DEBUG)

'''
Class helps to send data to the kafka
'''


class ProducerWithCallback(BaseProducer):

    '''
    Send message to the kafka server
    '''
    def send_message(self, message):
        self.producer.send(Constant.TOPIC_NAME, message).add_callback(
            self.on_send_success).add_callback(self.on_send_err)
        Logger.getLogger().info("Message sent")

    def on_send_success(self, record_metadata):
        logging.debug(f'Received metadata. Topic: {record_metadata.topic} \
            Partition: {record_metadata.partition} \
            Offset: {record_metadata.offset} \
            Timestamp: {record_metadata.timestamp}')

    def on_send_err(self, exception):
        logging.error(f"Error in sending message : {exception}")

    def __close__(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer = ProducerWithCallback()
    # for i in range(10):
    #     message = f"Message send at {i} position"
    #     producer.send_message(message.encode())
    producer.send_message(b"Hello, Kafka from the python")
    producer.__close__()
