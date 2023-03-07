package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Customer;
import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderEnriched;
import com.phuongheh.kafka.streams.avro.microservices.Payment;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
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

import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.*;
import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;


public class EmailService implements Service {
    private static final Logger log = LoggerFactory.getLogger(EmailService.class);
    private final String SERVICE_APP_ID = getClass().getSimpleName();

    private KafkaStreams streams;
    private final Emailer emailer;

    public EmailService(final Emailer emailer) {
        this.emailer = emailer;
    }

    @Override
    public void start(final String bootstrapServers,
                      final String stateDir,
                      final Properties defaultConfig) {
        streams = processStreams(bootstrapServers, stateDir, defaultConfig);
        streams.cleanUp();
        final CountDownLatch startLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
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
        log.info("Started Service " + SERVICE_APP_ID);

    }

    private KafkaStreams processStreams(final String bootstrapServers,
                                        final String stateDir,
                                        final Properties defaultConfig) {

        final StreamsBuilder builder = new StreamsBuilder();

        //Create the streams/tables for the join
        final KStream<String, Order> orders = builder.stream(ORDERS.name(),
                Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()));
        final KStream<String, Payment> payments = builder.stream(PAYMENTS.name(),
                Consumed.with(PAYMENTS.keySerde(), PAYMENTS.valueSerde()))
                //Rekey payments to be by OrderId for the windowed join
                .selectKey((s, payment) -> payment.getOrderId());
        final GlobalKTable<Long, Customer> customers = builder.globalTable(CUSTOMERS.name(),
                Consumed.with(CUSTOMERS.keySerde(), CUSTOMERS.valueSerde()));
        final StreamJoined<String, Order, Payment> serdes = StreamJoined
                .with(ORDERS.keySerde(), ORDERS.valueSerde(), PAYMENTS.valueSerde());
        orders.join(payments, EmailTuple::new,
                JoinWindows.of(Duration.ofMinutes(1)), serdes)
                .join(customers,
                        (key1, tuple) -> tuple.order.getCustomerId(),
                        EmailTuple::setCustomer)
                .peek((key, emailTuple)
                        -> emailer.sendEmail(emailTuple)
                );

        orders.join(customers, (orderId, order) -> order.getCustomerId(), (order, customer) -> new OrderEnriched(order.getId(), order.getCustomerId(), customer.getLevel()))
                .to((orderId, orderEnriched, record) -> orderEnriched.getCustomerLevel(), Produced.with(ORDERS_ENRICHED.keySerde(), ORDERS_ENRICHED.valueSerde()));

        return new KafkaStreams(builder.build(),
                baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
    }

    public static void main(final String[] args) throws Exception {
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
            formatter.printHelp("Email Service", opts);
            return;
        }
        final EmailService service = new EmailService(new LoggingEmailer());

        final Properties defaultConfig = Optional.ofNullable(cl.getOptionValue("config-file", null))
                .map(path -> {
                    try {
                        return buildPropertiesFromConfigFile(path);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(new Properties());


        final String schemaRegistryUrl = cl.getOptionValue("schema-registry", DEFAULT_SCHEMA_REGISTRY_URL);
        defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        Schemas.configureSerdes(defaultConfig);

        service.start(
                cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
                cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
                defaultConfig);
        addShutdownHookAndBlock(service);
    }

    private static class LoggingEmailer implements Emailer {

        @Override
        public void sendEmail(final EmailTuple details) {
            //In a real implementation we would do something a little more useful
            log.warn("Sending email: \nCustomer:{}\nOrder:{}\nPayment{}", details.customer, details.order, details.payment);
        }
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

    interface Emailer {
        void sendEmail(EmailTuple details);
    }

    public static class EmailTuple {

        final public Order order;
        final public Payment payment;
        public Customer customer;

        public EmailTuple(final Order order, final Payment payment) {
            this.order = order;
            this.payment = payment;
        }

        EmailTuple setCustomer(final Customer customer) {
            this.customer = customer;
            return this;
        }

        @Override
        public String toString() {
            return "EmailTuple{" +
                    "order=" + order +
                    ", payment=" + payment +
                    ", customer=" + customer +
                    '}';
        }
    }
}
