import logging
from kafka import KafkaProducer
from utils.constants import Constant

logging.basicConfig(level=logging.DEBUG)

class BaseProducer:
    def __init__(self):
        logging.info("Connecting to the kafka server...")
        self.__create_producer()

    '''
    Function will create the producer with the specified server
    '''
    def __create_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=Constant.BOOTSTRAP_SERVER_URL)
            logging.info("Connected to the server...")
        except Exception as e:
            logging.error(
                "Error while connecting to the server , {0}".format(e))
