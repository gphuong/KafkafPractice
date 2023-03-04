package com.phuongheh.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

public class PageViewRegionExample {
    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, GenericRecord> views = builder.stream("PageViews");
        final KStream<String, GenericRecord> viewsByUser = views.map(new KeyValueMapper<String, GenericRecord, KeyValue<? extends String, ? extends GenericRecord>>() {
            @Override
            public KeyValue<? extends String, ? extends GenericRecord> apply(String s, GenericRecord genericRecord) {
                return new KeyValue<>(genericRecord.get("user").toString(), genericRecord);
            }
        });
        final KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");
        final KTable<String, String> userRegions = userProfiles.mapValues(new ValueMapper<GenericRecord, String>() {
            @Override
            public String apply(GenericRecord genericRecord) {
                return genericRecord.get("region").toString();
            }
        });
        final InputStream pageViewRegionSchema = PageViewRegionLambdaExample.class.getClassLoader()
                .getResourceAsStream("com/phuongheh/kafka/streams/pageviewregion.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

        final KTable<Windowed<String>, Long> viewByRegion = viewsByUser.leftJoin(userRegions, new ValueJoiner<GenericRecord, String, GenericRecord>() {
            @Override
            public GenericRecord apply(GenericRecord genericRecord, String region) {
                final GenericRecord viewRegion = new GenericData.Record(schema);
                viewRegion.put("user", genericRecord.get("user"));
                viewRegion.put("page", genericRecord.get("page"));
                viewRegion.put("region", region);
                return viewRegion;
            }
        }).map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
            @Override
            public KeyValue<String, GenericRecord> apply(String user, GenericRecord viewByRegion) {
                return new KeyValue<>(viewByRegion.get("region").toString(), viewByRegion);
            }
        }).groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .count();
        final KStream<String, Long> viewsByRegionForConsole = viewByRegion.toStream(new KeyValueMapper<Windowed<String>, Long, String>() {
            @Override
            public String apply(Windowed<String> windowedRegion, Long count) {
                return windowedRegion.toString();
            }
        });
        viewsByRegionForConsole.to("PageViewByRegion", Produced.with(stringSerde, longSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
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
