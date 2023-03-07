package com.phuongheh.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class UserRegionLambdaExample {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamConfigurations = new Properties();
        streamConfigurations.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-region-lambda-example");
        streamConfigurations.put(StreamsConfig.CLIENT_ID_CONFIG, "user-region-lambda-example");
        streamConfigurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamConfigurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfigurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfigurations.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> userRegions = builder.table("UserRegions");

        final KTable<String, Long> regionCounts = userRegions.groupBy((userId, region) -> KeyValue.pair(region, region))
                .count()
                .filter((regionName, count) -> count >= 2);
        final KStream<String, Long> regionCountsForConsole = regionCounts.toStream().filter((regionName, count) -> count != null);
        regionCountsForConsole.to("LargeRegions", Produced.with(stringSerde, longSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfigurations);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
