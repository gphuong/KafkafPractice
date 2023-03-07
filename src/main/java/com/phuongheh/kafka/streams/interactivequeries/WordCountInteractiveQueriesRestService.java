package com.phuongheh.kafka.streams.interactivequeries;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Path("state")
public class WordCountInteractiveQueriesRestService {
    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private static final Logger log = LoggerFactory.getLogger(WordCountInteractiveQueriesRestService.class);

    public WordCountInteractiveQueriesRestService(KafkaStreams streams, HostInfo hostInfo) {
        this.streams = streams;
        this.hostInfo = hostInfo;
        metadataService = new MetadataService(streams);
    }

    @GET
    @Path("/keyvalue/{storeName}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public KeyValueBean bykey(@PathParam("storeName") final String storeName,
                              @PathParam("key") final String key) {
        final HostStoreInfo hostStoreInfo = streamsMetadataForStoreAndKey(storeName, key);
        if (!thisHost(hostStoreInfo)) {
            return fetchByKey(hostStoreInfo, "state/keyvalue/" + storeName + "/" + key);
        }

        final ReadOnlyKeyValueStore<String, Long> store =
                streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        if (store == null) {
            throw new NotFoundException();
        }
        final Long value = store.get(key);
        if (value == null) {
            throw new NotFoundException();
        }
        return new KeyValueBean(key, value);
    }

    @GET
    @Path("/keyvalues/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> allForStore(@PathParam("storeName") final String storeName) {
        return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
    }

    @GET
    @Path("/keyvalues/{storeName}/range/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> keyRangeForStore(@PathParam("storeName") final String storeName,
                                               @PathParam("from") final String from,
                                               @PathParam("to") final String to) {
        return rangeForKeyValueStore(storeName, store -> store.range(from, to));
    }

    @GET
    @Path("/windowed/{storeName}/{key}/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> windowedByKey(@PathParam("storeName") final String storeName,
                                            @PathParam("key") final String key,
                                            @PathParam("from") final Long from,
                                            @PathParam("to") final Long to) {
        final ReadOnlyWindowStore<String, Long> store =
                streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
        if (store == null) {
            throw new NotFoundException();
        }
        final WindowStoreIterator<Long> results = store.fetch(key, Instant.ofEpochMilli(from), Instant.ofEpochMilli(to));
        final List<KeyValueBean> windowResults = new ArrayList<>();
        while (results.hasNext()) {
            final KeyValue<Long, Long> next = results.next();
            windowResults.add(new KeyValueBean(key + "@" + next.key, next.value));
        }
        return windowResults;
    }

    @GET
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadata() {
        return metadataService.streamsMetadata();
    }

    @GET
    @Path("/instances/{storeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadataForStore(@PathParam("storeName") final String store) {
        return metadataService.streamsMetadataForStore(store);
    }

    @GET
    @Path("/instance/{storeName}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public HostStoreInfo streamsMetadataForStoreAndKey(@PathParam("storeName") final String store,
                                                       @PathParam("key") final String key) {
        return metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer());
    }

    private List<KeyValueBean> rangeForKeyValueStore(String storeName, Function<ReadOnlyKeyValueStore<String, Long>,
            KeyValueIterator<String, Long>> rangeFunction) {
        final ReadOnlyKeyValueStore<String, Long> store =
                streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        final List<KeyValueBean> results = new ArrayList<>();
        final KeyValueIterator<String, Long> range = rangeFunction.apply(store);
        while (range.hasNext()) {
            final KeyValue<String, Long> next = range.next();
            results.add(new KeyValueBean(next.key, next.value));
        }
        return results;
    }

    private KeyValueBean fetchByKey(HostStoreInfo host, String path) {
        return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<KeyValueBean>() {
                });
    }

    private boolean thisHost(HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    void start(final int port) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(port);
        jettyServer.addConnector(connector);

        context.start();
        try {
            jettyServer.start();
        } catch (final SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }
}
