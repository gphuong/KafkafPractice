package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.*;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import com.phuongheh.kafka.streams.utils.MonitoringInterceptorUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.phuongheh.kafka.streams.avro.microservices.OrderValidationResult.FAIL;
import static com.phuongheh.kafka.streams.avro.microservices.OrderValidationResult.PASS;
import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;

public class OrderDetailsService implements Service {
    private static final Logger log = LoggerFactory.getLogger(OrderDetailsService.class);
    private final String CONSUMER_GROUP_ID = getClass().getSimpleName();
    private KafkaConsumer<String, Order> consumer;
    private KafkaProducer<String, OrderValidation> producer;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running;

    private boolean eosEnabled = false;

    public static void main(String[] args) throws ParseException, InterruptedException {
        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                .longOpt("bootstrap-servers")
                .hasArgs()
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
            formatter.printHelp("Order Details Service", opts);
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

        final OrderDetailsService service = new OrderDetailsService();
        service.start(
                cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
                cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
                defaultConfig);
        addShutdownHookAndBlock(service);
    }

    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
        executorService.execute(() -> startService(bootstrapServers, defaultConfig));
        running = true;
        log.info("Started Service " + getClass().getSimpleName());
    }

    private void startService(String bootstrapServers, Properties defaultConfig) {
        startConsumer(bootstrapServers, defaultConfig);
        startProducer(bootstrapServers, defaultConfig);

        try {
            final Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
            consumer.subscribe(Collections.singletonList(Schemas.Topics.ORDERS.name()));
            if (eosEnabled) {
                producer.initTransactions();
            }
            while (running) {
                final ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                    if (eosEnabled) {
                        producer.beginTransaction();
                    }
                    for (final ConsumerRecord<String, Order> record : records) {
                        final Order order = record.value();
                        if (OrderState.CREATED.equals(order.getState())) {
                            producer.send(result(order, isValid(order) ? PASS : FAIL));
                            if (eosEnabled) {
                                recordOffset(consumedOffsets, record);
                            }
                        }
                    }
                    if (eosEnabled) {
                        producer.sendOffsetsToTransaction(consumedOffsets, new ConsumerGroupMetadata(CONSUMER_GROUP_ID));
                        producer.commitTransaction();
                    }
                }
            }

        } finally {
            close();
        }
    }

    private void close() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    private void recordOffset(Map<TopicPartition, OffsetAndMetadata> consumedOffsets, ConsumerRecord<String, Order> record) {
        final OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);
        consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), nextOffset);
    }

    private boolean isValid(Order order) {
        if (order.getQuantity() < 0) {
            return false;
        }
        if (order.getPrice() < 0) {
            return false;
        }
        return order.getProduct() != null;
    }

    private ProducerRecord<String, OrderValidation> result(final Order order,
                                                           final OrderValidationResult passOrFail) {
        return new ProducerRecord<>(Schemas.Topics.ORDER_VALIDATIONS.name(),
                order.getId(),
                new OrderValidation(order.getId(), OrderValidationType.ORDER_DETAILS_CHECK, passOrFail));
    }

    private void startProducer(String bootstrapServers, Properties defaultConfig) {
        final Properties producerConfig = new Properties();
        producerConfig.putAll(defaultConfig);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (eosEnabled) {
            producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "OrderDetailServiceInstance1");
        }
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_DOC, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-details-service-producer");
        MonitoringInterceptorUtils.maybeConfigureInterceptorsStreams(producerConfig);

        producer = new KafkaProducer<String, OrderValidation>(producerConfig,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());
    }

    private void startConsumer(String bootstrapServers, Properties defaultConfig) {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(defaultConfig);
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, !eosEnabled);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "order-details-service-consumer");
        MonitoringInterceptorUtils.maybeConfigureInterceptorsStreams(consumerConfig);

        consumer = new KafkaConsumer<String, Order>(consumerConfig, Schemas.Topics.ORDERS.keySerde().deserializer(),
                Schemas.Topics.ORDERS.valueSerde().deserializer());
    }

    @Override
    public void stop() {
        running
                = false;
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("Failed to stop " + getClass().getSimpleName() + " in 1000ms");
        }
        log.info(getClass().getSimpleName() + " was stopped");
    }
}
