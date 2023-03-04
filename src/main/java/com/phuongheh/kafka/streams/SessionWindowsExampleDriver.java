package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.PlayEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SessionWindowsExampleDriver {
    private static final int NUM_RECORDS_SENT = 8;

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        producePlayEvents(bootstrapServers, schemaRegistryUrl);
        consumeOutput(bootstrapServers);
    }

    static void consumeOutput(String bootstrapServers) {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "session_windows-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());

        final KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(consumerProps);
        consumer.subscribe(Collections.singleton(SessionWindowsExample.PLAY_EVENTS_PER_SESSION));
        int received = 0;
        while (received < NUM_RECORDS_SENT) {
            final ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            records.forEach(record -> System.out.println(record.key() + " = " + record.value()));
            received += records.count();
        }
        consumer.close();
    }

    static void producePlayEvents(String bootstrapServers, String schemaRegistryUrl) {
        final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        playEventSerializer.configure(serdeConfig, false);
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        final KafkaProducer<String, PlayEvent> playEventProducer
                = new KafkaProducer<String, PlayEvent>(producerProperties, Serdes.String().serializer(),
                playEventSerializer);
        final long start = System.currentTimeMillis();
        final long billEvenTime = start + SessionWindowsExample.INACTIVITY_GAP.toMillis() / 10;
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start,
                "jo",
                new PlayEvent(1L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                billEvenTime,
                "bill",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + SessionWindowsExample.INACTIVITY_GAP.toMillis() / 5,
                "sarah",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + SessionWindowsExample.INACTIVITY_GAP.toMillis() + 1,
                "jo",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + SessionWindowsExample.INACTIVITY_GAP.toMillis(),
                "bill",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + 2 * SessionWindowsExample.INACTIVITY_GAP.toMillis(),
                "sarah",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + SessionWindowsExample.INACTIVITY_GAP.toMillis() / 2,
                "jo",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + 3 * SessionWindowsExample.INACTIVITY_GAP.toMillis(),
                "bill",
                new PlayEvent(2L, 10L)));
        playEventProducer.send(new ProducerRecord<>(SessionWindowsExample.PLAY_EVENTS,
                null,
                start + 2 * SessionWindowsExample.INACTIVITY_GAP.toMillis(),
                "sarah",
                new PlayEvent(2L, 10L)));
        playEventProducer.close();
    }
}
