package com.phuongheh.kafka.streams.interactivequeries;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WordCountInteractiveQueriesExample {
    static final String TEXT_LINES_TOPIC = "TextLinesTopic";
    static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length > 2) {
            throw new IllegalArgumentException("usage: ... <portForRestEndPoint> [<bootstrap.servers> (optional)]");
        }
        final int port = Integer.parseInt(args[0]);
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-queries-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "interactive-queries-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, DEFAULT_HOST + ":" + port);
        final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

        final KafkaStreams streams = createStreams(streamsConfiguration);
    }

    private static KafkaStreams createStreams(Properties streamsConfiguration) {
        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(TEXT_LINES_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KGroupedStream<String, String> groupedByWord = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(stringSerde, stringSerde));
        groupedByWord.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-count")
                .withValueSerde(Serdes.Long()));
        groupedByWord.windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-word-count")
                        .withValueSerde(Serdes.Long()));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
