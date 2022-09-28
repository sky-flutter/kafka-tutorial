package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class StreamStarterApp {
    public static void main(String[] args) {
        Properties mProperties = new Properties();
        mProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        mProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        mProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        mProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        mProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        mProperties.put(StreamsConfig.STATE_DIR_CONFIG, "wordcount-application" + Long.valueOf(System.currentTimeMillis()).toString());


        StreamsBuilder mStreamBuilder = new StreamsBuilder();

        // 1. Stream from Kafka
        KStream<String, String> mWordCountInputStream = mStreamBuilder.stream("word-count-input");
        // 2. Map values to lowercase
        KTable<String, Long> counts = mWordCountInputStream.mapValues(value -> value.toLowerCase())
                // 3. Flatmap values split by space
                .flatMapValues(s -> Arrays.asList(s.split(" ")))
                // 4. Select key to apply a key
                .selectKey((key, value) -> value)
                // 5. Group by before key aggregation
                .groupBy((key, value) -> key)
                // 6. Count the occurrences
                .count(Named.as("Counts"));
        // 7. To in order to write the results back to kafka
        counts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(mStreamBuilder.build(), mProperties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        System.out.println(kafkaStreams);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
