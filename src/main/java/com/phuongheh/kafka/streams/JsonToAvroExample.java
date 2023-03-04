package com.phuongheh.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phuongheh.kafka.streams.avro.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class JsonToAvroExample {
    static final String JSON_SOURCE_TOPIC = "json-source";
    static final String AVRO_SINK_TOPIC = "avro-sink";

    public static void main(String[] args) {
        final String bootstrapServer = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildJsonToAvroStream(
                bootstrapServer,
                schemaRegistryUrl
        );
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams buildJsonToAvroStream(String bootstrapServer, String schemaRegistryUrl) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-to-avro-streams-conversion");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "json-to-avro-stream-conversion-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);
        final ObjectMapper objectMapper = new ObjectMapper();
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> jsonToAvroStream = builder.stream(JSON_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        jsonToAvroStream.mapValues(v -> {
            WikiFeed wikiFeed = null;
            try {
                final JsonNode jsonNode = objectMapper.readTree(v);
                wikiFeed = new WikiFeed(jsonNode.get("user").asText(), jsonNode.get("is_new").asBoolean(), jsonNode.get("content").asText());
            } catch (IOException io) {
                throw new RuntimeException(io);
            }
            return wikiFeed;
        }).filter((k, v) -> v != null).to(AVRO_SINK_TOPIC);
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
