package com.phuongheh.kafka.streams.microservices.domain;

import com.phuongheh.kafka.streams.avro.microservices.*;
import com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;


public class Schemas {

    public static SpecificAvroSerde<OrderValue> ORDER_VALUE_SERDE = new SpecificAvroSerde<>();

    public static class Topic<K, V> {

        private final String name;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            Topics.ALL.put(name, this);
        }

        public Serde<K> keySerde() {
            return keySerde;
        }

        public Serde<V> valueSerde() {
            return valueSerde;
        }

        public String name() {
            return name;
        }

        public String toString() {
            return name;
        }
    }

    public static class Topics {
        public final static Map<String, Topic<?, ?>> ALL = new HashMap<>();
        public static Topic<String, Order> ORDERS;
        public static Topic<String, OrderEnriched> ORDERS_ENRICHED;
        public static Topic<String, Payment> PAYMENTS;
        public static Topic<Long, Customer> CUSTOMERS;
        public static Topic<Product, Integer> WAREHOUSE_INVENTORY;
        public static Topic<String, OrderValidation> ORDER_VALIDATIONS;

        static {
            createTopics();
        }

        private static void createTopics() {
            ORDERS = new Topic<>("orders", Serdes.String(), new SpecificAvroSerde<>());
            ORDERS_ENRICHED = new Topic<>("orders-enriched", Serdes.String(), new SpecificAvroSerde<>());
            PAYMENTS = new Topic<>("payments", Serdes.String(), new SpecificAvroSerde<>());
            CUSTOMERS = new Topic<>("customers", Serdes.Long(), new SpecificAvroSerde<>());
            ORDER_VALIDATIONS = new Topic<>("order-validations", Serdes.String(), new SpecificAvroSerde<>());
            WAREHOUSE_INVENTORY = new Topic<>("warehouse-inventory", new MicroserviceUtils.ProductTypeSerde(), Serdes.Integer());
            ORDER_VALUE_SERDE = new SpecificAvroSerde<>();
        }
    }

    public static Map<String, ?> buildSchemaRegistryConfigMap(final Properties config) {
        final HashMap<String, String> map = new HashMap<>();
        if (config.containsKey(SCHEMA_REGISTRY_URL_CONFIG))
            map.put(SCHEMA_REGISTRY_URL_CONFIG, config.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        if (config.containsKey(BASIC_AUTH_CREDENTIALS_SOURCE))
            map.put(BASIC_AUTH_CREDENTIALS_SOURCE, config.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
        if (config.containsKey(USER_INFO_CONFIG))
            map.put(USER_INFO_CONFIG, config.getProperty(USER_INFO_CONFIG));
        return map;
    }

    public static void configureSerdes(final Properties config) {
        Topics.createTopics(); //wipe cached schema registry
        for (final Topic<?, ?> topic : Topics.ALL.values()) {
            configureSerde(topic.keySerde(), config, true);
            configureSerde(topic.valueSerde(), config, false);
        }
        configureSerde(ORDER_VALUE_SERDE, config, false);
    }

    private static void configureSerde(final Serde<?> serde, final Properties config, final Boolean isKey) {
        if (serde instanceof SpecificAvroSerde) {
            serde.configure(buildSchemaRegistryConfigMap(config), isKey);
        }
    }
}
