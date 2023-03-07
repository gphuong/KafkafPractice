package com.phuongheh.kafka.streams.interactivequeries.kafkamusic;

import com.phuongheh.kafka.streams.avro.Song;
import com.phuongheh.kafka.streams.avro.SongPlayCount;
import com.phuongheh.kafka.streams.interactivequeries.HostStoreInfo;
import com.phuongheh.kafka.streams.interactivequeries.MetadataService;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
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
import java.util.ArrayList;
import java.util.List;

@Path("kafka-music")
public class MusicPlayRestService {
    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(MusicPlayRestService.class);

    public MusicPlayRestService(KafkaStreams streams, HostInfo hostInfo) {
        this.streams = streams;
        this.hostInfo = hostInfo;
        this.metadataService = new MetadataService(streams);
    }

    @GET
    @Path("/charts/genre/{genre}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<SongPlayCountBean> genreCharts(@PathParam("genre") final String genre) {
        final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(KafkaMusicExample.TOP_FIVE_SONGS_BY_GENRE_STORE, genre, new StringSerializer());
        if (!thisHost(host)) {
            return fetchSongPlayCount(host, "kafka-music/charts/genre/" + genre);
        }
        return topFiveSongs(genre.toLowerCase(), KafkaMusicExample.TOP_FIVE_SONGS_BY_GENRE_STORE);
    }

    @GET
    @Path("/charts/top-five")
    @Produces(MediaType.APPLICATION_JSON)
    public List<SongPlayCountBean> topFive() {
        final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(KafkaMusicExample.TOP_FIVE_SONGS_STORE, KafkaMusicExample.TOP_FIVE_KEY, new StringSerializer());
        if (!thisHost(host)) {
            return fetchSongPlayCount(host, "kafka-music/charts/top-five/");
        }
        return topFiveSongs(KafkaMusicExample.TOP_FIVE_KEY, KafkaMusicExample.TOP_FIVE_SONGS_STORE);
    }

    @GET
    @Path("/song/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public SongBean song(@PathParam("id") final Long songId) {
        final ReadOnlyKeyValueStore<Long, Song> songStore = streams.store(StoreQueryParameters.fromNameAndType(KafkaMusicExample.ALL_SONGS, QueryableStoreTypes.keyValueStore()));
        final Song song = songStore.get(songId);
        if (song == null) {
            throw new NotFoundException(String.format("Song with id [%d] was not found", songId));
        }
        return new SongBean(song.getArtist(), song.getAlbum(), song.getName());
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

    void start() throws Exception {
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
        connector.setPort(hostInfo.port());
        jettyServer.addConnector(connector);
        context.start();
        try {
            jettyServer.start();
        } catch (SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    private List<SongPlayCountBean> topFiveSongs(String key, String storeName) {
        final ReadOnlyKeyValueStore<String, KafkaMusicExample.TopFiveSongs> topFiveStores =
                streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        final KafkaMusicExample.TopFiveSongs value = topFiveStores.get(key);
        if (value == null) {
            throw new NotFoundException(String.format("Unable to find value in %s for key %s", storeName, key));
        }
        final List<SongPlayCountBean> results = new ArrayList<>();
        for (final SongPlayCount songPlayCount : value) {
            final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(KafkaMusicExample.ALL_SONGS, songPlayCount.getSongId(), serializer);
            if (!thisHost(host)) {
                final SongBean song = client.target(String.format("http://%s:%d/kafka-music/song/%d", host.getHost(), host.getPort(), songPlayCount.getSongId()))
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .get(SongBean.class);
                results.add(new SongPlayCountBean(song.getArtist(), song.getAlbum(), song.getName(), songPlayCount.getPlays()));
            } else {
                final ReadOnlyKeyValueStore<Long, Song> songStore =
                        streams.store(StoreQueryParameters.fromNameAndType(KafkaMusicExample.ALL_SONGS, QueryableStoreTypes.keyValueStore()));
                final Song song = songStore.get(songPlayCount.getSongId());
                results.add(new SongPlayCountBean(song.getArtist(), song.getAlbum(), song.getName(), songPlayCount.getPlays()));
            }
        }
        return results;
    }

    private List<SongPlayCountBean> fetchSongPlayCount(HostStoreInfo host, String path) {
        return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<List<SongPlayCountBean>>() {
                });
    }

    private boolean thisHost(HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) && host.getPort() == hostInfo.port();
    }
}
