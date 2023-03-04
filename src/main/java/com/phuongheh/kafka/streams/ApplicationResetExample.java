package com.phuongheh.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ApplicationResetExample {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-reset-demo");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "application-reset-demo-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final boolean doReset = args.length > 1 && args[1].equals("--reset");
        final KafkaStreams streams = buildKafkaStreams(streamsConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        if (doReset) {
            streams.cleanUp();
        }

        startKafkaStreamsSynchronously(streams);
    }

    private static void startKafkaStreamsSynchronously(KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });
        streams.start();
        try {
            latch.wait();
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

    }

    private static KafkaStreams buildKafkaStreams(Properties streamsConfiguration) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream("my-input-topic");
        input.selectKey((key, value) -> value
                .split(" ")[0])
                .groupByKey()
                .count()
                .toStream()
                .to("my-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
