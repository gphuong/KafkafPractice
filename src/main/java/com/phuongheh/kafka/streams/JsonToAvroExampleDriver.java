package com.phuongheh.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phuongheh.kafka.streams.avro.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class JsonToAvroExampleDriver {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        produceJsonInputs(bootstrapServers, schemaRegistryUrl);
        consumeAvroOutput(bootstrapServers, schemaRegistryUrl);
    }

    private static void consumeAvroOutput(String bootstrapServers, String schemaRegistryUrl) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "json-to-avro-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        final KafkaConsumer<String, WikiFeed> consumer = new KafkaConsumer<String, WikiFeed>(consumerProperties);
        consumer.subscribe(Collections.singleton(JsonToAvroExample.AVRO_SINK_TOPIC));
        while (true) {
            final ConsumerRecords<String, WikiFeed> consumerRecords = consumer.poll(Duration.ofSeconds(120));
            for (final ConsumerRecord<String, WikiFeed> consumerRecord : consumerRecords) {
                final WikiFeed wikiFeed = consumerRecord.value();
                System.out.println("Converted Avro Record " + wikiFeed.getUser() + " " + wikiFeed.getIsNew() + " " + wikiFeed.getContent());
            }
        }
    }

    private static void produceJsonInputs(String bootstrapServers, String schemaRegistryUrl) {
        final String[] users = {"Black Knight", "Sir Robin", "Knight Who Says Ni"};
        final ObjectMapper objectMapper = new ObjectMapper();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        try (final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            Arrays.stream(users).map(user -> {
                String jsonMap;
                try {
                    final Map<String, String> map = new HashMap<>();
                    map.put("user", user);
                    map.put("is_new", "true");
                    map.put("content", "flesh wound");
                    jsonMap = objectMapper.writeValueAsString(map);
                } catch (final IOException e) {
                    jsonMap = "{}";
                }
                return jsonMap;
            }).forEach(record -> producer.send(new ProducerRecord<>(JsonToAvroExample.JSON_SOURCE_TOPIC, null, record)));
            producer.flush();
        }
    }
}
