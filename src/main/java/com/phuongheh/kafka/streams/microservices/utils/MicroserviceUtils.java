package com.phuongheh.kafka.streams.microservices.utils;

import com.phuongheh.kafka.streams.avro.microservices.Product;
import com.phuongheh.kafka.streams.microservices.Service;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import com.phuongheh.kafka.streams.utils.MonitoringInterceptorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MicroserviceUtils {
    private static final Logger log = LoggerFactory.getLogger(MicroserviceUtils.class);
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static Properties buildPropertiesFromConfigFile(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            properties.load(inputStream);
        }
        return properties;
    }

    public static Properties baseStreamsConfig(final String bootstrapServers,
                                               final String stateDir,
                                               final String appId,
                                               final Properties defaultConfig) {
        return baseStreamsConfig(bootstrapServers, stateDir, appId, false, defaultConfig);
    }

    public static Properties baseStreamsConfigEOS(final String bootstrapServers,
                                                  final String stateDir,
                                                  final String appId,
                                                  final Properties defaultConfig) {
        return baseStreamsConfig(bootstrapServers, stateDir, appId, true, defaultConfig);
    }

    private static Properties baseStreamsConfig(String bootstrapServers, String stateDir, String appId, boolean enableEOS, Properties defaultConfig) {
        final Properties config = new Properties();
        config.putAll(defaultConfig);
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEOS ? "exactly_once" : "at_least_once");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 300000);
        MonitoringInterceptorUtils.maybeConfigureInterceptorsStreams(config);
        return config;
    }

    public static class CustomRocksDBConfig implements RocksDBConfigSetter {

        @Override
        public void setConfig(String storeName, Options options, Map<String, Object> configs) {
            final int compactionParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 2);
            options.setIncreaseParallelism(compactionParallelism);
        }

        @Override
        public void close(String s, Options options) {

        }
    }

    public static final class ProductTypeSerde implements Serde<Product> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<Product> serializer() {
            return new Serializer<Product>() {
                @Override
                public byte[] serialize(String s, Product product) {
                    return product.toString().getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {

                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public Deserializer<Product> deserializer() {
            return new Deserializer<Product>() {
                @Override
                public Product deserialize(String s, byte[] bytes) {
                    return Product.valueOf(new String(bytes, StandardCharsets.UTF_8));
                }

                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {

                }

                @Override
                public void close() {

                }
            };
        }
    }

    public static void setTimeout(final long timeout, final AsyncResponse asyncResponse) {
        asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(resp -> resp.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("HTTP GET timed out after " + timeout + "ms\n")
                        .build()
        ));
    }

    public static Server startJetty(final int port, final Object binding) {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        final Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(binding);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        try {
            jettyServer.start();
        } catch (Exception ex) {
            throw new RuntimeException();
        }
        log.info("Listening on " + jettyServer.getURI());
        return jettyServer;
    }

    public static <T> KafkaProducer startProducer(final String bootstrapServers,
                                                  final Schemas.Topic<String, T> topic,
                                                  final Properties defaultConfig) {
        final Properties producerConfig = new Properties();
        producerConfig.putAll(defaultConfig);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_DOC, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-sender");
        MonitoringInterceptorUtils.maybeConfigureInterceptorsStreams(producerConfig);
        return new KafkaProducer(producerConfig,
                topic.keySerde().serializer(),
                topic.valueSerde().serializer());
    }

    public static void addShutdownHookAndBlock(final Service service) throws InterruptedException {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> service.stop());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {

            }
        }));
        Thread.currentThread().join();
    }
}
