package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.*;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDERS;
import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;

public class FraudService implements Service {
    private static final Logger log = LoggerFactory.getLogger(FraudService.class);
    private final String SERVICE_APP_ID = getClass().getSimpleName();

    private static final int FRAUD_LIMIT = 2000;
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
        streams = processStreams(bootstrapServers, stateDir, defaultConfig);
        streams.cleanUp();
        final CountDownLatch startLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
                ;
            }
        });
        streams.start();
        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Streams never finished rebalancing on startup");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams processStreams(String bootstrapServers, String stateDir, Properties defaultConfig) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Order> orders = builder.stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()))
                .filter((id, order) -> OrderState.CREATED.equals(order.getState()));

        final KTable<Windowed<Long>, OrderValue> aggregate = orders.groupBy((id, order) -> order.getCustomerId(), Grouped.with(Serdes.Long(), ORDERS.valueSerde()))
                .windowedBy(SessionWindows.with(Duration.ofHours(1)))
                .aggregate(OrderValue::new,
                        (custId, order, total) -> new OrderValue(order,
                                total.getValue() + order.getQuantity() * order.getPrice()),
                        (k, a, b) -> simpleMerge(a, b),
                        Materialized.with(null, Schemas.ORDER_VALUE_SERDE));
        final KStream<String, OrderValue> ordersWithTotals = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key())
                .filter((k, v) -> v != null)
                .selectKey((id, orderValue) -> orderValue.getOrder().getId());

        final Map<String, KStream<String, OrderValue>> forks = ordersWithTotals.split(Named.as("limit-"))
                .branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT, Branched.as("above"))
                .branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT, Branched.as("below"))
                .noDefaultBranch();
        forks.get("limit-above").mapValues(
                orderValue -> new OrderValidation(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL))
                .to(ORDER_VALIDATIONS.name(), Produced.with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde()));
        forks.get("limit-below").mapValues(
                orderValue -> new OrderValidation(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS))
                .to(ORDER_VALIDATIONS.name(), Produced.with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde()));
        final Properties props = baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig);
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return new KafkaStreams(builder.build(), props);
    }

    private OrderValue simpleMerge(OrderValue a, OrderValue b) {
        return new OrderValue(b.getOrder(), (a == null ? 0D : a.getValue()) + b.getValue());
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                .longOpt("bootstrap-servers")
                .hasArg()
                .desc("Kafka cluster bootstrap server string (ex: broker:9092")
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
            formatter.printHelp("Fraud Service", opts);
            return;
        }
        final FraudService service = new FraudService();
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
        Schemas.configureSerde(defaultConfig);

        service.start(cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
                cl.getOptionValue("state-dir", "/tmp/kafka-streams-example"),
                defaultConfig);
        addShutdownHookAndBlock(service);
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
