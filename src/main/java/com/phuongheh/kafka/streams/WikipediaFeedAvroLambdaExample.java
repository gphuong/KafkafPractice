package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

public class WikipediaFeedAvroLambdaExample {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildWikipediaFeed(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams");
    }

    private static KafkaStreams buildWikipediaFeed(String bootstrapServers, String schemaRegistryUrl, String stateDir) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-avro-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-avro-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, WikiFeed> feeds = builder.stream(WikipediaFeedAvroExample.WIKIPEDIA_FEED);
        final KTable<String, Long> aggregated = feeds.filter((dummy, value) -> value.getIsNew())
                .map((key, value) -> new KeyValue<>(value.getUser(), value))
                .groupByKey()
                .count();
        aggregated.toStream().to(WikipediaFeedAvroExample.WIKIPEDIA_STATS, Produced.with(stringSerde, longSerde));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
