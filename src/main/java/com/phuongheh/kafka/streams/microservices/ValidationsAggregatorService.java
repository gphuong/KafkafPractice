package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderState;
import com.phuongheh.kafka.streams.avro.microservices.Order.newBuilder;
import com.phuongheh.kafka.streams.avro.microservices.OrderValidation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.phuongheh.kafka.streams.avro.microservices.OrderState.VALIDATED;
import static com.phuongheh.kafka.streams.avro.microservices.OrderValidationResult.PASS;
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
        final CountDownLatch startLatch = new CountDownLatch(1);
        streams = aggregateOrderValidations(bootstrapServers, stateDir, defaultConfig);
    }

    private KafkaStreams aggregateOrderValidations(String bootstrapServers, String stateDir, Properties defaultConfig) {
        final int numberOfRules = 3;
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, OrderValidation> validations = builder.stream(ORDER_VALIDATIONS.name(), serdes1);
        final KStream<String, Order> orders = builder.stream(ORDERS.name(), serdes2)
                .filter((id, order) -> OrderState.CREATED.equals(order.getState()));
        validations
                .groupByKey(serdes3)
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .aggregate(
                        () -> 0L,
                        (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
                        (k, a, b) -> b == null ? a : b,
                        Materialized.with(null, Serdes.Long())
                ).toStream((windowedKey, total) -> windowedKey.key())
                .filter((k1, v) -> v != null)
                .filter((k, total) -> total >= numberOfRules)
                .join(orders, (id, order) ->
                                newBuilder(order).setState(VALIDATED).build()
                        , JoinWindows.of(Duration.ofMinutes(5)), serdes4)
                .to(ORDERS.name(), serdes5);
    }

    @Override
    public void stop() {

    }
}
