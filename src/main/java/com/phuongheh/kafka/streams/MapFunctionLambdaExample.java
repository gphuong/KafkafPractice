package com.phuongheh.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class MapFunctionLambdaExample {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:29092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<byte[], String> textLines = builder.stream("TextLineTopic", Consumed.with(byteArraySerde, stringSerde));

        final KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(v -> v.toUpperCase());
        uppercasedWithMapValues.to("UppercasedTextLinesTopic");

        final KStream<byte[], String> uppercasedWithMap = textLines.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));
        final KStream<String, String> originalAndUppercased = textLines.map((key, value) -> KeyValue.pair(value, value.toUpperCase()));
        originalAndUppercased.to("OriginalAndUppercasedTopic", Produced.with(stringSerde, stringSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
