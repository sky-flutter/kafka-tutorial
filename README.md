# Kafka Word Count Streams

<p>This repository provides the sample implementation about the kafka streams. Kafka stream generally used for data processing and transformation</p>


### Prerequisite to execute project
- Kafka
- Zookeeper

### Run project

1. **Create topic** 
   ```
   kafka-topics.sh --bootstrap-server localhost:<kafka-server-port> --create --topic word-count-input --partitions 2 --replication-factor 1
   ``` 
   ```
   kafka-topics.sh --bootstrap-server localhost:<kafka-server-port> --create --topic word-count-output --partitions 2 --replication-factor 1
   ```
2. **Start producer on the word-count-input topic**
   ```
   kafka-console-producer.sh --bootstrap-server localhost:<kafka-server-port> --topic word-count-input
   ``` 
3. **Start consumer on the word-count-output topic**
    ```
    kafka-console-consumer.sh --bootstrap-server localhost:9091 \
    --topic word-count-output --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
      ``` 
4. Run the `StreamStarterApp.java` file