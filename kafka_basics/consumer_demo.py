import logging
from kafka import KafkaConsumer
from utils.constants import Constant

# logging.basicConfig(level=logging.DEBUG)


class ConsumerDemo:
    def __init__(self):
        print("Connecting to the kafka server...")
        self.__create_consumer()

    def __create_consumer(self):
        try:
            self.consumer = KafkaConsumer(Constant.TOPIC_NAME,
                                          group_id='first_group1', bootstrap_servers=Constant.BOOTSTRAP_SERVER_URL,)
            self.consumer.subscribe([Constant.TOPIC_NAME, ])
            print("Connected to the kafka server...")
        except Exception as e:
            print(str(e))

    def consume_message(self):
        try:
            while True:
                records = self.consumer.poll(1000)
                for topic_data, consumer_records in records.items():
                    for consumer_record in consumer_records:

                        print("Received message: " +
                              str(consumer_record.value.decode('utf-8')) + " On Partition : "+str(topic_data.partition))
                continue
        except Exception as e:
            print(str(e))
            self.consumer.close()


if __name__ == "__main__":
    consumerDemo = ConsumerDemo()
    consumerDemo.consume_message()
