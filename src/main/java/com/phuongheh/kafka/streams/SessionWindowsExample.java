package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.PlayEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

public class SessionWindowsExample {
    static final String PLAY_EVENTS = "play-events";
    static final Duration INACTIVITY_GAP = Duration.ofMinutes(30);
    static final String PLAY_EVENTS_PER_SESSION = "play-events-per-session";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = new KafkaStreams(
                buildTopology(singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)),
                streamsConfig(bootstrapServers, "/tmp/kafka-streams")
        );
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties streamsConfig(String bootstrapServers, String stateDir) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-windows-example");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "session-windows-example-client");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return config;
    }

    static Topology buildTopology(Map<String, String> serdeConfig) {
        final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
        playEventSerde.configure(serdeConfig, false);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(PLAY_EVENTS, Consumed.with(Serdes.String(), playEventSerde))
                .groupByKey(Grouped.with(Serdes.String(), playEventSerde))
                .windowedBy(SessionWindows.with(INACTIVITY_GAP))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(PLAY_EVENTS_PER_SESSION)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
                .to(PLAY_EVENTS_PER_SESSION, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
