package com.phuongheh.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class SumLambdaExampleDriver {
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        produceInput(bootstrapServers);
        consumeOutput(bootstrapServers);
    }

    private static void consumeOutput(final String bootstrapServers) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sum-lambda-example-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<Integer, Integer>(properties);
        consumer.subscribe(Collections.singleton(SumLambdaExample.SUM_OF_ODD_NUMBERS_TOPIC));
        while (true) {
            final ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (final ConsumerRecord<Integer, Integer> record : records) {
                System.out.println("Current sum of odd numbers is:" + record.value());
            }
        }
    }

    private static void produceInput(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final KafkaProducer<Void, Integer> producer = new KafkaProducer<Void, Integer>(props);
        IntStream.range(0, 100)
                .mapToObj(val -> new ProducerRecord(SumLambdaExample.NUMBERS_TOPIC, null, val));
        producer.flush();
    }
}
