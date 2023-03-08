package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderState;
import com.phuongheh.kafka.streams.avro.microservices.OrderValidation;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.phuongheh.kafka.streams.avro.microservices.Order.newBuilder;
import static com.phuongheh.kafka.streams.avro.microservices.OrderState.VALIDATED;
import static com.phuongheh.kafka.streams.avro.microservices.OrderValidationResult.FAIL;
import static com.phuongheh.kafka.streams.avro.microservices.OrderValidationResult.PASS;
import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDERS;
import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;

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
        streams.cleanUp();

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
        validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
                .join(orders, (id, order) ->
                                newBuilder(order).setState(OrderState.FAILED).build(),
                        JoinWindows.of(Duration.ofMinutes(5)), serdes7)
                .groupByKey(serdes6)
                .reduce((order, v1) -> order)
                .toStream().to(ORDERS.name(), Produced.with(ORDERS.keySerde(), ORDERS.valueSerde()));
        return new KafkaStreams(builder.build(),
                baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        final Options opts = new Options();
        opts.addOption(Option.builder("b").longOpt("bootstrap-servers").hasArg().desc("Kafka cluster bootstrap server string (ex: broker:9092)").build());
        opts.addOption(Option.builder("s").longOpt("schema-registry").hasArg().desc("Schema Registry URL").build());
        opts.addOption(Option.builder("c").longOpt("config-file").hasArg().desc("Java properties file with configurations for Kafka Clients").build());
        opts.addOption(Option.builder("t").longOpt("state-dir").hasArg().desc("The directory for state storage").build());
        opts.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());
        final CommandLine cl = new DefaultParser().parse(opts, args);
        if (cl.hasOption("h")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Validator Aggregator Service", opts);
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

        final ValidationsAggregatorService service = new ValidationsAggregatorService();
        service.start(cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
                cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
                defaultConfig);
        addShutdownHookAndBlock(service);
    }
}
