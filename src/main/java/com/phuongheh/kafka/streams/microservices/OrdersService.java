package com.phuongheh.kafka.streams.microservices;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderState;
import com.phuongheh.kafka.streams.interactivequeries.HostStoreInfo;
import com.phuongheh.kafka.streams.interactivequeries.MetadataService;
import com.phuongheh.kafka.streams.microservices.domain.Schemas;
import com.phuongheh.kafka.streams.microservices.domain.beans.OrderBean;
import com.phuongheh.kafka.streams.microservices.utils.Paths;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.scala.Serdes;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.phuongheh.kafka.streams.microservices.domain.Schemas.Topics.ORDERS;
import static com.phuongheh.kafka.streams.microservices.domain.beans.OrderBean.fromBean;
import static com.phuongheh.kafka.streams.microservices.domain.beans.OrderBean.toBean;
import static com.phuongheh.kafka.streams.microservices.utils.MicroserviceUtils.*;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;

@Path("v1")
public class OrdersService implements Service {
    private static final Logger log = LoggerFactory.getLogger(OrdersService.class);
    private static final String CALL_TIMEOUT = "10000";
    private static final String ORDERS_STORE_NAME = "orders-store";
    private final String SERVICE_APP_ID = getClass().getSimpleName();
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private final String host;
    private int port;
    private KafkaStreams streams = null;
    private MetadataService metadataService;
    private KafkaProducer<String, Order> producer;

    private final Map<String, FilteredResponse> outstandingRequests = new ConcurrentHashMap<>();

    public OrdersService(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public OrdersService(String host) {
        this(host, 0);
    }

    private StreamsBuilder createOrdersMaterializedView() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()), Materialized.as(ORDERS_STORE_NAME))
                .toStream().foreach(this::maybeCompleteLongPollGet);
        return builder;
    }

    private void maybeCompleteLongPollGet(String id, Order order) {
        final FilteredResponse<String, Order> callback = outstandingRequests.get(id);
        if (callback != null && callback.predicate.test(id, order)) {
            callback.asyncResponse.resume(toBean(order));
        }
    }

    @GET
    @ManagedAsync
    @Path("/order/{id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void getWithTimeout(@PathParam("id") String id,
                               @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) Long timeout,
                               @Suspended AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);
        final HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);
        if (hostForKey == null)
            return;
        if (thisHost(hostForKey)) {
            fetchLocal(id, asyncResponse, (k, v) -> true);
        } else {
            final String path = new Paths(hostForKey.getHost(), hostForKey.getPort()).urlGet(id);
            fetchFromOtherHost(path, asyncResponse, timeout);
        }
    }

    @GET
    @ManagedAsync
    @Path("orders/{id}/validated")
    public void getPostValidationWithTimeout(@PathParam("id") String id,
                                             @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) Long timeout,
                                             @Suspended AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);
        final HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);
        if (hostForKey == null) {
            return;
        }
        if (thisHost(hostForKey)) {
            fetchLocal(id, asyncResponse, (k, v) -> (v.getState() == OrderState.VALIDATED || v.getState() == OrderState.FAILED));
        } else {
            fetchFromOtherHost(new Paths(hostForKey.getHost(), hostForKey.getPort()).urlGetValidated(id), asyncResponse, timeout);
        }
    }

    @POST
    @ManagedAsync
    @Path("/orders")
    @Consumes(MediaType.APPLICATION_JSON)
    public void submitOrder(OrderBean order, @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) Long timeout,
                            @Suspended AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);
        final Order bean = fromBean(order);
        producer.send(new ProducerRecord<>(ORDERS.name(), bean.getId(), bean), callback(asyncResponse, bean.getId()));
    }

    private Callback callback(AsyncResponse asyncResponse, String id) {
        return ((recordMetadata, e) -> {
            if (e != null) {
                asyncResponse.resume(e);
            } else {
                try {
                    final Response uri = Response.created(new URI("/v1/orders/" + id)).build();
                    asyncResponse.resume(uri);
                } catch (URISyntaxException e2) {
                    e2.printStackTrace();
                }
            }
        });
    }

    private void fetchFromOtherHost(String path, AsyncResponse asyncResponse, Long timeout) {
        log.info("Chaining GET to a different instance: " + path);
        try {
            final OrderBean bean = client.target(path)
                    .queryParam("timeout", timeout)
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            asyncResponse.resume(bean);
        } catch (Exception swallowed) {
            log.warn("GET failed.", swallowed);
        }
    }

    private void fetchLocal(String id, AsyncResponse asyncResponse, Predicate<String, Order> predicate) {
        log.info("running GET on this node");
        try {
            final Order order = ordersStore().get(id);
            if (order == null || !predicate.test(id, order)) {
                log.info("Delaying get as order not present for id " + id);
                outstandingRequests.put(id, new FilteredResponse(asyncResponse, predicate));
            } else {
                asyncResponse.resume(toBean(order));
            }
        } catch (InvalidStateStoreException e) {
            outstandingRequests.put(id, new FilteredResponse(asyncResponse, predicate));
        }
    }

    private ReadOnlyKeyValueStore<String, Order> ordersStore() {
        return streams.store(StoreQueryParameters.fromNameAndType(ORDERS_STORE_NAME, QueryableStoreTypes.keyValueStore()));
    }

    private boolean thisHost(HostStoreInfo host) {
        return host.getHost().equals(this.host) && host.getPort() == port;
    }

    private HostStoreInfo getKeyLocationOrBlock(String id, AsyncResponse asyncResponse) {
        HostStoreInfo locationOfKey;
        while (locationMetadataIsUnavailable(locationOfKey = getHostForOrderId(id))) {
            if (asyncResponse.isDone()) {
                return null;
            }
            try {
                Thread.sleep(Math.min(Long.parseLong(CALL_TIMEOUT), 200));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return locationOfKey;
    }

    private boolean locationMetadataIsUnavailable(HostStoreInfo hostWithKey) {
        return NOT_AVAILABLE.host().equals(hostWithKey.getHost()) && NOT_AVAILABLE.port() == hostWithKey.getPort();
    }

    private HostStoreInfo getHostForOrderId(String id) {
        return metadataService.streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, id, Serdes.String().serializer());
    }

    @Override
    public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
        jettyServer = startJetty(port, this);
        port = jettyServer.getURI().getPort();
        producer = startProducer(bootstrapServers, ORDERS, defaultConfig);
        defaultConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streams = startKStreams(bootstrapServers, defaultConfig);
        log.info("Started Service " + getClass().getSimpleName());
        log.info("Order Service listening at: " + jettyServer.getURI().toString());
    }

    private KafkaStreams startKStreams(String bootstrapServers, Properties defaultConfig) {
        final KafkaStreams streams = new KafkaStreams(
                createOrdersMaterializedView().build(),
                config(bootstrapServers, defaultConfig));
        metadataService = new MetadataService(streams);
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
                throw new RuntimeException("Streams never finished rebancing on startup");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return streams;
    }

    private Properties config(String bootstrapServers, Properties defaultConfig) {
        final String stateDir = defaultConfig.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        final Properties props = baseStreamsConfig(
                bootstrapServers,
                stateDir != null ? stateDir : "/tmp/kafka-streams",
                SERVICE_APP_ID,
                defaultConfig);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + port);
        return props;
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    void cleanLocalState() {
        if (streams != null) {
            streams.cleanUp();
        }
    }

    public int port() {
        return port;
    }

    static class FilteredResponse<K, V> {
        private final AsyncResponse asyncResponse;
        private final Predicate<K, V> predicate;

        public FilteredResponse(AsyncResponse asyncResponse, Predicate<K, V> predicate) {
            this.asyncResponse = asyncResponse;
            this.predicate = predicate;
        }
    }

    public static void main(String[] args) throws ParseException, InterruptedException {
        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                .longOpt("bootstrap-servers").hasArg().desc("Kafka cluster bootstrap server string").build())
                .addOption(Option.builder("s").longOpt("schema-registry").hasArg().desc("Schema Registry URL").build())
                .addOption(Option.builder("h").longOpt("hostname").hasArg().desc("This services HTTP host name").build())
                .addOption(Option.builder("p").longOpt("port").hasArg().desc("This service HTTP port").build())
                .addOption(Option.builder("c").longOpt("config-file").hasArg().desc("Java properties file with configuration for Kafka Clients").build())
                .addOption(Option.builder("d").longOpt("state-dir").hasArg().desc("The directory for state storage").build())
                .addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());

        final CommandLine cl = new DefaultParser().parse(opts, args);
        if (cl.hasOption("h")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Order Service", opts);
            return;
        }
        final String bootstrapServers = cl.getOptionValue("bootstrap-server", DEFAULT_BOOTSTRAP_SERVERS);
        final String restHostname = cl.getOptionValue("hostname", "localhost");
        final int restPort = Integer.parseInt(cl.getOptionValue("port", "5432"));
        final String stateDir = cl.getOptionValue("state-dir", "/tmp/kafka-streams");

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

        final OrdersService service = new OrdersService(restHostname, restPort);
        service.start(bootstrapServers, stateDir, defaultConfig);
        addShutdownHookAndBlock(service);
    }
}
