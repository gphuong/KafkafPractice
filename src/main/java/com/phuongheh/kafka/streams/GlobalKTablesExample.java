package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.Customer;
import com.phuongheh.kafka.streams.avro.EnrichedOrder;
import com.phuongheh.kafka.streams.avro.Order;
import com.phuongheh.kafka.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class GlobalKTablesExample {
    static final String ORDER_TOPIC = "order";
    static final String CUSTOMER_TOPIC = "customer";
    static final String PRODUCT_TOPIC = "product";
    static final String CUSTOMER_STORE = "customer-store";
    static final String PRODUCT_STORE = "product-store";
    static final String ENRICHED_ORDER_TOPIC = "enriched-order";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 0 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = createStream(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams-global-tables");
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KafkaStreams createStream(String bootstrapServers, String schemaRegistryUrl, String stateDir) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-tables-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-tables-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<EnrichedOrder> enrichedOrderSerde = new SpecificAvroSerde<>();
        enrichedOrderSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, Order> ordersStream = builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Long(), orderSerde));
        final GlobalKTable<Long, Customer> customers = builder.globalTable(CUSTOMER_TOPIC,
                Materialized.<Long, Customer, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_STORE)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(customerSerde));
        final GlobalKTable<Long, Product> products = builder.globalTable(PRODUCT_TOPIC, Materialized.<Long, Product, KeyValueStore<Bytes, byte[]>>
                as(PRODUCT_STORE).withKeySerde(Serdes.Long()).withValueSerde(productSerde));
        final KStream<Long, CustomerOrder> customerOrderStream = ordersStream.join(customers,
                (orderId, order) -> order.getCustomerId(), (order, customer) -> new CustomerOrder(customer, order));
        final KStream<Long, EnrichedOrder> enrichedOrdersStream = customerOrderStream.join(products,
                (orderId, customerOrder) -> customerOrder.productId(),
                (customerOrder, product) -> new EnrichedOrder(product, customerOrder.customer, customerOrder.order));
        enrichedOrdersStream.to(ENRICHED_ORDER_TOPIC, Produced.with(Serdes.Long(), enrichedOrderSerde));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private static class CustomerOrder {
        private final Customer customer;
        private final Order order;

        public CustomerOrder(Customer customer, Order order) {
            this.customer = customer;
            this.order = order;
        }

        long productId() {
            return order.getProductId();
        }
    }
}
