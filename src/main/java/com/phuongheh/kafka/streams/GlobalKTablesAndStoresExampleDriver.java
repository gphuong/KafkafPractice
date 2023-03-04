package com.phuongheh.kafka.streams;

import com.phuongheh.kafka.streams.avro.Customer;
import com.phuongheh.kafka.streams.avro.EnrichedOrder;
import com.phuongheh.kafka.streams.avro.Order;
import com.phuongheh.kafka.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.*;

import static com.phuongheh.kafka.streams.GlobalKTablesExample.*;

public class GlobalKTablesAndStoresExampleDriver {
    private static final Random RANDOM = new Random();
    private static final int RECORDS_TO_GENERATE = 100;

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        generateCustomers(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
        generateProducts(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
        generateOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE);
        receiveEnrichedOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
    }

    private static void receiveEnrichedOrders(
            String bootstrapServers,
            String schemaRegistryUrl,
            int expected) {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        final KafkaConsumer<Long, EnrichedOrder> consumer = new KafkaConsumer<Long, EnrichedOrder>(consumerProps);
        consumer.subscribe(Collections.singleton(ENRICHED_ORDER_TOPIC));
        int received = 0;
        while (received < expected) {
            final ConsumerRecords<Long, EnrichedOrder> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            records.forEach(record -> System.out.println(record.value()));
            received += records.count();
        }
        consumer.close();

    }

    private static List<Order> generateOrders(String bootstrapServers,
                                              String schemaRegistryUrl,
                                              int numCustomers,
                                              int numProducts,
                                              int count) {
        final SpecificAvroSerde<Order> orderSerde = createSerde(schemaRegistryUrl);
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        final KafkaProducer<Long, Order> producer = new KafkaProducer<Long, Order>(producerProperties, Serdes.Long().serializer(), orderSerde.serializer());
        final List<Order> allOrders = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            final long customerId = RANDOM.nextInt(numCustomers);
            final long productId = RANDOM.nextInt(numProducts);
            final Order order = new Order(customerId,
                    productId, RANDOM.nextLong());
            allOrders.add(order);
            producer.send(new ProducerRecord<>(ORDER_TOPIC, i, order));
        }
        producer.close();
        return allOrders;
    }

    private static List<Product> generateProducts(
            String bootstrapServers,
            String schemaRegistryUrl,
            int count) {
        final SpecificAvroSerde<Product> productSerde = createSerde(schemaRegistryUrl);
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final KafkaProducer<Long, Product> producer = new KafkaProducer<Long, Product>(producerProperties, Serdes.Long().serializer(), productSerde.serializer());
        final List<Product> allProducts = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            final Product product = new Product(randomString(10),
                    randomString(RECORDS_TO_GENERATE), randomString(20));
            allProducts.add(product);
            producer.send(new ProducerRecord<>(PRODUCT_TOPIC, i, product));
        }
        producer.close();
        return allProducts;
    }

    private static List<Customer> generateCustomers(
            String bootstrapServers,
            String schemaRegistryUrl,
            int count) {
        final SpecificAvroSerde<Customer> customerSerde = createSerde(schemaRegistryUrl);
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final KafkaProducer<Long, Customer> customerProducer =
                new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), customerSerde.serializer());
        final List<Customer> allCustomers = new ArrayList<>();
        final String[] genders = {"male", "female", "unknown"};
        final Random random = new Random();
        for (long i = 0; i < count; i++) {
            final Customer customer = new Customer(randomString(10),
                    genders[random.nextInt(genders.length)],
                    randomString(20));
            allCustomers.add(customer);
            customerProducer.send(new ProducerRecord<>(CUSTOMER_TOPIC, i, customer));
        }
        customerProducer.close();
        return allCustomers;
    }

    private static String randomString(int length) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < length; i++) {
            b.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".charAt(RANDOM.nextInt
                    ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".length())));
        }
        return b.toString();
    }

    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(String schemaRegistryUrl) {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        );
        serde.configure(serdeConfig, false);
        return serde;
    }
}
