package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderValidation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDERS;
import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;

public class ValidationsAggregatorService implements Service {
    private static final Logger log = LoggerFactory.getLogger(ValidationsAggregatorService.class);
    private final String SERVICE_APP_ID = getClass().getSimpleName();
    private final Consumed<String, OrderValidation> serdes1 = Consumed.with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
    private final Consumed<String, Order> serdes2 = Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde());
    private final Grouped<String, OrderValidation> serdes3 = Grouped.with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
    private final StreamJoined<String, Long, Order> serdes4 = StreamJoined.with(ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde());
    private final Produced<String, Order> serdes5 = Produced.with(ORDERS.keySerde(), ORDERS.valueSerde());
    private final Grouped<String, Order> serdes6 = Grouped.with(ORDERS.keySerde(), ORDERS.valueSerde());
    private final StreamJoined<String, OrderValidation, Order> serdes7 = StreamJoined.with(ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde());

    private KafkaStreams streams;


    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {

    }

    @Override
    public void stop() {

    }
}
