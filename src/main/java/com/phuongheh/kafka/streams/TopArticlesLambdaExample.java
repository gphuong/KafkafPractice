package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.utils.PriorityQueueSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
import java.util.*;

public class TopArticlesLambdaExample {
    static final String TOP_NEW_PER_INDUSTRY_TOPIC = "TopNewsPerIndustry";
    static final String PAGE_VIEWS = "pageViews";
    static final Duration windowSize = Duration.ofHours(1);

    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildTopArticlesStream(
                bootstrapServers,
                schemaRegistryUrl,
                "/tmp/kafka-streams");
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams buildTopArticlesStream(
            String bootstrapServers,
            String schemaRegistryUrl,
            String stateDir) throws IOException {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "top-articles-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );
        final Serde<GenericRecord> keyAvroSerde = new GenericAvroSerde();
        keyAvroSerde.configure(serdeConfig, true);

        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(serdeConfig, false);

        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<byte[], GenericRecord> views = builder.stream(PAGE_VIEWS);

        final InputStream statsSchema = TopArticlesLambdaExample.class.getClassLoader()
                .getResourceAsStream("com/phuongheh/kakfa/streams/pageviewstats.avsc");
        final Schema schema = new Schema.Parser().parse(statsSchema);

        final KStream<GenericRecord, GenericRecord> articlesView = views.filter((dummy, record) -> isArticle(record))
                .map((dummy, article) -> {
                    final GenericRecord clone = new GenericData.Record(article.getSchema());
                    clone.put("user", "user");
                    clone.put("page", article.get("page"));
                    clone.put("industry", article.get("industry"));
                    return new KeyValue<>(clone, clone);
                });
        final KTable<Windowed<GenericRecord>, Long> viewCounts = articlesView
                .groupByKey(Grouped.with(keyAvroSerde, valueAvroSerde))
                .windowedBy(TimeWindows.of(windowSize))
                .count();

        final Comparator<GenericRecord> comparator =
                (o1, o2) -> (int) ((Long) o2.get("count") - (Long) o1.get("count"));
        final KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts
                .groupBy(
                        (windowedArticle, count) -> {
                            final Windowed<String> windowedIndustry =
                                    new Windowed<>(windowedArticle.key().get("industry").toString(),
                                            windowedArticle.window());
                            final GenericRecord viewStats = new GenericData.Record(schema);
                            viewStats.put("page", windowedArticle.key().get("page"));
                            viewStats.put("user", "user");
                            viewStats.put("industry", windowedArticle.key().get("industry"));
                            viewStats.put("count", count);
                            return new KeyValue<>(windowedIndustry, viewStats);
                        },
                        Grouped.with(windowedStringSerde, valueAvroSerde))
                .aggregate(() -> new PriorityQueue<>(comparator),
                        (windowedIndustry, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },
                        (windowedIndustry, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        }, Materialized.with(windowedStringSerde,
                                new PriorityQueueSerde<>(comparator, valueAvroSerde))
                );
        final int topN = 100;
        final KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        final GenericRecord record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append(record.get("page").toString());
                        sb.append("\n");
                    }
                    return sb.toString();
                });
        topViewCounts.toStream().to(TOP_NEW_PER_INDUSTRY_TOPIC, Produced.with(windowedStringSerde, stringSerde));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private static boolean isArticle(GenericRecord record) {
        final Utf8 flags = (Utf8) record.get("flags");
        if (flags == null) {
            return false;
        }
        return flags.toString().contains("ARTICLE");

    }
}
