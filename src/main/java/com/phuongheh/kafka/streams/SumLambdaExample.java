package com.phuongheh.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class SumLambdaExample {
    static final String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
    static final String NUMBERS_TOPIC = "numbers-topic";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sum-lambda-example-client");

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Topology topology = getTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.cleanUp();
        streams.start();
    }

    private static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Void, Integer> input = builder.stream(NUMBERS_TOPIC);
        final KTable<Integer, Integer> sumOfOddNumbers = input.filter((k, v) -> v % 2 != 0)
                .selectKey((k, v) -> 1)
                .groupByKey()
                .reduce((v1, v2) -> v1 + v2);
        sumOfOddNumbers.toStream().to(SUM_OF_ODD_NUMBERS_TOPIC);
        return builder.build();
    }
}
