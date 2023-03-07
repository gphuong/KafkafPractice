package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.*;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;

public class InventoryService implements Service {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SERVICE_APP_ID = "InventoryService";
    private static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
        streams = processStreams(bootstrapServers, stateDir, defaultConfig);
        streams.cleanUp();
        final CountDownLatch startLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
            }
        });
        streams.start();
        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Streams never finished rebalancing on startup");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams processStreams(String bootstrapServers, String stateDir, Properties defaultConfig) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Order> orders = builder.stream(Schemas.Topics.ORDERS.name(),
                Consumed.with(Schemas.Topics.ORDERS.keySerde(), Schemas.Topics.ORDERS.valueSerde()));
        final KTable<Product, Integer> warehouseInventory = builder
                .table(Schemas.Topics.WAREHOUSE_INVENTORY.name(),
                        Consumed.with(Schemas.Topics.WAREHOUSE_INVENTORY.keySerde(), Schemas.Topics.WAREHOUSE_INVENTORY.valueSerde()));
        final StoreBuilder<KeyValueStore<Product, Long>> reservedStock = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(RESERVED_STOCK_STORE_NAME),
                        Schemas.Topics.WAREHOUSE_INVENTORY.keySerde(), Serdes.Long())
                .withLoggingEnabled(new HashMap<>());
        builder.addStateStore(reservedStock);
        orders.selectKey((id, order) -> order.getProduct())
                .filter((id, order) -> OrderState.CREATED.equals(order.getState()))
                .join(warehouseInventory, KeyValue::new, Joined.with(Schemas.Topics.WAREHOUSE_INVENTORY.keySerde(), Schemas.Topics.ORDERS.valueSerde(), Serdes.Integer()))
                .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME)
                .to(Schemas.Topics.ORDER_VALIDATIONS.name(), Produced.with(Schemas.Topics.ORDER_VALIDATIONS.keySerde(), Schemas.Topics.ORDER_VALIDATIONS.valueSerde()));
        return new KafkaStreams(builder.build(),
                MicroserviceUtils.baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
    }

    private static class InventoryValidator implements Transformer<Product, KeyValue<Order, Integer>, KeyValue<String, OrderValidation>> {
        private KeyValueStore<Product, Long> reservedStocksStore;

        @Override
        public void init(ProcessorContext processorContext) {
            reservedStocksStore = processorContext.getStateStore(RESERVED_STOCK_STORE_NAME);
        }

        @Override
        public KeyValue<String, OrderValidation> transform(Product product, KeyValue<Order, Integer> orderAndStock) {
            final OrderValidation validated;
            final Order order = orderAndStock.key;
            final Integer warehouseStockCount = orderAndStock.value;
            Long reserved = reservedStocksStore.get(order.getProduct());
            if (reserved == null) {
                reserved = 0L;
            }
            if (warehouseStockCount - reserved - order.getQuantity() >= 0) {
                reservedStocksStore.put(order.getProduct(), reserved + order.getQuantity());
                validated = new OrderValidation(order.getId(), OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS);
            } else {
                validated = new OrderValidation(order.getId(), OrderValidationType.INVENTORY_CHECK, OrderValidationResult.FAIL);
            }
            return KeyValue.pair(validated.getOrderId(), validated);

        }

        @Override
        public void close() {

        }
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                .longOpt("bootstrap-servers")
                .hasArg()
                .desc("Kafka cluster bootstrap server string (ex: broker:9092)")
                .build());
        opts.addOption(Option.builder("s")
                .longOpt("schema-registry")
                .hasArg()
                .desc("Schema Registry URL")
                .build());
        opts.addOption(Option.builder("c")
                .longOpt("config-file")
                .hasArg()
                .desc("Java properties file with configurations for Kafka Clients")
                .build());
        opts.addOption(Option.builder("t")
                .longOpt("state-dir")
                .hasArg()
                .desc("The directory for state storage")
                .build());
        opts.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());
        final CommandLine cl = new DefaultParser().parse(opts, args);
        if (cl.hasOption("h")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Inventory Service", opts);
            return;
        }

        final Properties defaultConfig = Optional.ofNullable(cl.getOptionValue("config-file", null))
                .map(path -> {
                    try {
                        return buildPropertiesFromConfigFile(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).orElse(new Properties());
        final String schemaRegistryUrl = cl.getOptionValue("schema-registry", DEFAULT_SCHEMA_REGISTRY_URL);
        defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        Schemas.configureSerdes(defaultConfig);

        final InventoryService service = new InventoryService();
        service.start(cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
                cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
                defaultConfig);
        addShutdownHookAndBlock(service);
    }
}
