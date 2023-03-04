package com.phuongheh.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class PageViewRegionExampleDriver {
    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        produceInputs(bootstrapServers, schemaRegistryUrl);
        consumeOutput(bootstrapServers);
    }

    private static void consumeOutput(String bootstrapServers) {
        final String resultTopic = "PageViewByRegion";
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "pageview-region-lambda-example-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (final KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(consumerProperties)) {
            consumer.subscribe(Collections.singleton(resultTopic));
            while (true) {
                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.key() + ":" + consumerRecord.value());
                }
            }
        }
    }

    private static void produceInputs(String bootstrapServers, String schemaRegistryUrl) throws IOException {
        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
        final String[] regions = {"europe", "usa", "asia", "africa"};
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final GenericRecordBuilder pageViewBuilder = new GenericRecordBuilder(loadSchema("pageview.avsc"));
        final GenericRecordBuilder userProfileBuilder = new GenericRecordBuilder(loadSchema("userprofile.avsc"));

        final String pageViewTopic = "PageViews";
        final String userProfileTopic = "UserProfiles";

        final Random random = new Random();
        pageViewBuilder.set("industry", "eng");
        try (final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props)) {
            for (final String user : users) {
                userProfileBuilder.set("experience", "some");
                userProfileBuilder.set("region", regions[random.nextInt(regions.length)]);
                producer.send(new ProducerRecord<>(userProfileTopic, user, userProfileBuilder.build()));
                for (int i = 0; i < random.nextInt(10); i++) {
                    pageViewBuilder.set("user", user);
                    pageViewBuilder.set("page", "index.html");
                    final GenericData.Record record = pageViewBuilder.build();
                    producer.send(new ProducerRecord<>(pageViewTopic, null, record));
                }
            }
        }
    }

    private static Schema loadSchema(String name) throws IOException {
        try (final InputStream input = PageViewRegionLambdaExample.class.getClassLoader()
                .getResourceAsStream("com/phuongheh/kafka/streams/" + name)) {
            return new Schema.Parser().parse(input);
        }
    }
}
