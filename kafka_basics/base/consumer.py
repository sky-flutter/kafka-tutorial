import logging
from kafka import KafkaConsumer
from utils.constants import Constant

logging.basicConfig(level=logging.DEBUG)


class BaseConsumer:
    def __init__(self):
        logging.info("Connecting to the kafka server...")
        self.__create_consumer()

    def __create_consumer(self):
        self.consumer = KafkaConsumer(Constant.TOPIC_NAME,
            group_id='first_group', auto_offset_reset='latest', bootstrap_servers=[Constant.BOOTSTRAP_SERVER_URL],)
        self.consumer.subscribe([Constant.TOPIC_NAME,])
        logging.info("Connected to the kafka server...")
