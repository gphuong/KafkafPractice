package com.phuongheh.kafka.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SecureKafkaStreamExample {
    public static void main(String[] args) {
        final String secureBootstrapServers = args.length > 0 ? args[0] : "localhost:9093";
        final Properties streamConfiguration = new Properties();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "secure-kafka-streams-app");
        streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "secure-kakfa-streams-app-client");
        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, secureBootstrapServers);
        streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        streamConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        streamConfiguration.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.truststore.jks");
        streamConfiguration.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        streamConfiguration.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.keystore.jks");
        streamConfiguration.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        streamConfiguration.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("secure-input").to("secure-output");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }
}
