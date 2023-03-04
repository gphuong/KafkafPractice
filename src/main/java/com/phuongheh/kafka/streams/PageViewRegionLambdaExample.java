package com.phuongheh.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

public class PageViewRegionLambdaExample {
    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final Properties streamConfiguration = new Properties();

        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-lambda-example");
        streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-lambda-example-client");
        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, GenericRecord> views = builder.stream("PageViews");

        final KStream<String, GenericRecord> viewByUser =
                views.map((dummy, record) -> new KeyValue<>(record.get("user").toString(), record));

        final KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");
        final KTable<String, String> userRegions = userProfiles.mapValues(record -> record.get("region").toString());
        final InputStream pageViewRegionSchema = PageViewRegionLambdaExample.class.getClassLoader()
                .getResourceAsStream("avro/io/confluent/examples/streams/pageviewregion.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);
        final KTable<Windowed<String>, Long> viewsByRegion = viewByUser
                .leftJoin(userRegions, (view, region) -> {
                    final GenericRecord viewRegion = new GenericData.Record(schema);
                    viewRegion.put("user", view.get("user"));
                    viewRegion.put("page", view.get("page"));
                    viewRegion.put("region", region);
                    return viewRegion;
                }).map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").toString(), viewRegion))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMillis(1)))
                .count();
        final KStream<String, Long> viewByRegionForConsole = viewsByRegion
                .toStream((windowedRegion, count) -> windowedRegion.toString());
        viewByRegionForConsole.to("PageViewsByRegion", Produced.with(stringSerde, longSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
