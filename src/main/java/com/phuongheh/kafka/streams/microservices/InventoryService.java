package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderState;
import com.phuongheh.kafka.streams.avro.microservices.OrderValidation;
import com.phuongheh.kafka.streams.avro.microservices.Product;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
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

import java.util.HashMap;
import java.util.Properties;

public class InventoryService implements Service {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SERVICE_APP_ID = "InventoryService";
    private static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    streams = processStreams(bootstrapServers, stateDir, defaultConfig);
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
        orders.selectKey((id, order)->order.getProduct())
                .filter((id, order)-> OrderState.CREATED.equals(order.getState()))
                .join(warehouseInventory, KeyValue::new, Joined.with(Schemas.Topics.WAREHOUSE_INVENTORY.keySerde(), Schemas.Topics.ORDERS.valueSerde(), Serdes.Integer()))
                .transform(InventoryValidator::new)
    }
private static class InventoryValidator implements Transformer<Product, KeyValue<Order, Integer>, KeyValue<String, OrderValidation>>{
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

        
        return null;
    }

    @Override
    public void close() {

    }
}
    @Override
    public void stop() {

    }
}
