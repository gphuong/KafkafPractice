package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.Customer;
import com.phuongheh.kafka.streams.avro.EnrichedOrder;
import com.phuongheh.kafka.streams.avro.Order;
import com.phuongheh.kafka.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class GlobalStoreExample {
    static final String ORDER_TOPIC = "order";
    static final String CUSTOMER_TOPIC = "customer";
    static final String PRODUCT_TOPIC = "product";
    static final String CUSTOMER_STORE = "customer-store";
    static final String PRODUCT_STORE = "product-store";
    static final String ENRICHED_ORDER_TOPIC = "enriched-order";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://locahost:8081";
        final KafkaStreams streams = createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-stream-global-stores");
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams createStreams(
            String bootstrapServers,
            String schemaRegistryUrl,
            String stateDir) {
        final Properties streamConfiguration = new Properties();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-example");
        streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-stores-example-client");

        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        streamConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        orderSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<EnrichedOrder> enrichedOrderSerde = new SpecificAvroSerde<>();
        enrichedOrderSerde.configure(serdeConfig, false);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Order> orderStream = builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Long(), orderSerde));
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(CUSTOMER_STORE), Serdes.Long(), customerSerde),
                GlobalStoreExample.CUSTOMER_TOPIC,
                Consumed.with(Serdes.Long(), customerSerde),
                () -> new GlobalStoreUpdater<>(CUSTOMER_STORE));
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(PRODUCT_STORE), Serdes.Long(), productSerde),
                PRODUCT_TOPIC,
                Consumed.with(Serdes.Long(), productSerde),
                () -> new GlobalStoreUpdater<>(PRODUCT_STORE));

        final KStream<Long, EnrichedOrder> enrichedOrderKStream = orderStream.transformValues(()
                -> new ValueTransformer<Order, EnrichedOrder>() {
            private KeyValueStore<Long, Customer> customerStores;
            private KeyValueStore<Long, Product> productStores;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext processorContext) {
                customerStores = processorContext.getStateStore(CUSTOMER_STORE);
                productStores = processorContext.getStateStore(PRODUCT_STORE);
            }

            @Override
            public EnrichedOrder transform(Order order) {
                final Customer customer = customerStores.get(order.getCustomerId());
                final Product product = productStores.get(order.getProductId());
                return new EnrichedOrder(product, customer, order);
            }

            @Override
            public void close() {

            }
        });
        enrichedOrderKStream.to(ENRICHED_ORDER_TOPIC, Produced.with(Serdes.Long(), enrichedOrderSerde));
        return new KafkaStreams(builder.build(), streamConfiguration);
    }

    private static class GlobalStoreUpdater<K, V> implements Processor<K, V, Void, Void> {
        private final String storeName;

        public GlobalStoreUpdater(String storeName) {
            this.storeName = storeName;
        }

        private KeyValueStore<K, V> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<K, V> record) {
            store.put(record.key(), record.value());
        }

        @Override
        public void close() {

        }
    }
}
