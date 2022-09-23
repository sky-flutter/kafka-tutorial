import json
from opensearchpy import OpenSearch, helpers
from kafka import KafkaConsumer


class OpenSearchConsumer:
    index_name = "wikimedia"
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }
    }

    def __init__(self):
        self.create_opensearch_client()

    def create_opensearch_client(self):
        host = 'localhost'
        port = 9200
        auth = ('admin', 'admin')
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,  # enables gzip compression for request bodies
            http_auth=auth,
            use_ssl=False,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

    def create_get_index(self):
        if not self.client.indices.exists(self.index_name):
            response = self.client.indices.create(
                index=self.index_name, body=self.index_body)
            print(response)
        else:
            print("Index already exists")

    def create_bulk_request(self, consumer_records):
        for records in consumer_records:
            yield {"_index": self.index_name, "_source": records}

    def create_consumer(self):
        self.create_get_index()
        consumer = KafkaConsumer(bootstrap_servers="localhost:9091",
                                 group_id="consumer_opensearch_demo", auto_offset_reset="latest", enable_auto_commit=False, value_deserializer=lambda m: json.loads(m))
        consumer.subscribe(topics=["wikimedia.recentchange"])
        bulk_request = []
        while (True):
            records = consumer.poll(3000)
            bulk_request = []

            for topicdata, consumer_records in records.items():
                # for consumer_record in consumer_records:
                bulk_request.append(self.create_bulk_request(consumer_records))
                    # try:
                    #     id = json.loads(consumer_record.value)['meta']['id']
                    #     response = self.client.index(
                    #         index=self.index_name,
                    #         body=consumer_record.value,
                    #         id=id
                    #     )
                    #     print(id)
                    # except:
                    #     continue
            if bulk_request:
                helpers.bulk(self.client,bulk_request)
                
            consumer.commit()
            print("Offset have been committed")


if __name__ == "__main__":
    opensearch_consumer = OpenSearchConsumer()
    opensearch_consumer.create_consumer()
