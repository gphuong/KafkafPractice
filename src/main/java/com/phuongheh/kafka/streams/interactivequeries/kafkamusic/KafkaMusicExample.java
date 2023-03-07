package com.phuongheh.kafka.streams.interactivequeries.kafkamusic;

import com.phuongheh.kafka.streams.avro.PlayEvent;
import com.phuongheh.kafka.streams.avro.Song;
import com.phuongheh.kafka.streams.avro.SongPlayCount;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.*;
import java.util.*;

import static java.util.Collections.singletonMap;

public class KafkaMusicExample {
    private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;
    private static final String SONG_PLAY_COUNT_STORE = "song-play-count";
    static final String PLAY_EVENTS = "play-events";
    static final String ALL_SONGS = "all-songs";
    static final String SONG_FEED = "song-feed";
    static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-songs-by-genre";
    static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
    static final String TOP_FIVE_KEY = "all";

    private static final String DEFAULT_REST_ENDPOINT_HOSTNAME = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length > 4) {
            throw new IllegalArgumentException("usage: ... <portForRestEndpoint> " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")]" +
                    "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")]" +
                    "[<hostnameForRestEndpoint> (optional, default: " + DEFAULT_REST_ENDPOINT_HOSTNAME + ")]");
        }
        final int restEndpointPort = Integer.parseInt(args[0]);
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
        final String restEndpointHostname = args.length > 3 ? args[3] : DEFAULT_REST_ENDPOINT_HOSTNAME;
        final HostInfo restEndpoint = new HostInfo(restEndpointHostname, restEndpointPort);

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
        System.out.println("REST endpoint at http://" + restEndpointHostname + ":" + restEndpointPort);

        final KafkaStreams streams = new KafkaStreams(
                buildTopology(singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)),
                streamsConfig(bootstrapServers, restEndpointPort, "/tpm/kafka-streams", restEndpointHostname));
        streams.cleanUp();
        streams.start();
        final MusicPlayRestService restService = startRestProxy(streams, restEndpoint);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try{
                restService.stop();
                streams.close();
            }catch (final Exception e){

            }
        }));
    }

    private static MusicPlayRestService startRestProxy(KafkaStreams streams, HostInfo hostInfo) throws Exception {
        final MusicPlayRestService interactiveQueriesRestService = new MusicPlayRestService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
    }

    private static Properties streamsConfig(String bootstrapServers,
                                            int applicationServerPort,
                                            String stateDir,
                                            String host) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-music-charts");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + applicationServerPort);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        final String metadataMaxAgeMs = System.getProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG);
        if (metadataMaxAgeMs != null) {
            try {
                final int value = Integer.parseInt(metadataMaxAgeMs);
                streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, value);
                System.out.println("Set consumer configuration " + ConsumerConfig.METADATA_MAX_AGE_CONFIG + " to " + value);
            } catch (final NumberFormatException ignored) {
            }
        }
        return streamsConfiguration;
    }

    private static Topology buildTopology(Map<String, String> serdeConfig) {
        final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
        playEventSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<Song> keySongSerde = new SpecificAvroSerde<>();
        keySongSerde.configure(serdeConfig, true);

        final SpecificAvroSerde<Song> valueSongSerde = new SpecificAvroSerde<>();
        valueSongSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<SongPlayCount> songPlayCountSerde = new SpecificAvroSerde<>();
        songPlayCountSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PlayEvent> playEvents = builder.stream(
                PLAY_EVENTS,
                Consumed.with(Serdes.String(), playEventSerde)
        );

        final KTable<Long, Song>
                songTable =
                builder.table(SONG_FEED, Materialized.<Long, Song, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(ALL_SONGS)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(valueSongSerde));

        final KStream<Long, PlayEvent> playsBySongId =
                playEvents.filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
                        .map((key, value) -> KeyValue.pair(value.getSongId(), value));

        final KStream<Long, Song> songPlays = playsBySongId.leftJoin(songTable,
                (value1, song) -> song,
                Joined.with(Serdes.Long(), playEventSerde, valueSongSerde));

        final KTable<Song, Long> songPlayCounts = songPlays.groupBy((songId, song) -> song, Grouped.with(keySongSerde, valueSongSerde))
                .count(Materialized.<Song, Long, KeyValueStore<Bytes, byte[]>>as(SONG_PLAY_COUNT_STORE)
                        .withKeySerde(valueSongSerde)
                        .withValueSerde(Serdes.Long()));
        final TopFiveSerde topFiveSerde = new TopFiveSerde();

        songPlayCounts.groupBy((song, plays) ->
                        KeyValue.pair(song.getGenre().toLowerCase(),
                                new SongPlayCount(song.getId(), plays)),
                Grouped.with(Serdes.String(), songPlayCountSerde))
                .aggregate(TopFiveSongs::new, (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        (aggKey, value, aggregate) -> {
                            aggregate.remove(value);
                            return aggregate;
                        },
                        Materialized.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(TOP_FIVE_SONGS_BY_GENRE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(topFiveSerde));
        return builder.build();
    }

    private static class TopFiveSerde implements Serde<TopFiveSongs> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<TopFiveSongs> serializer() {
            return new Serializer<TopFiveSongs>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {

                }

                @Override
                public byte[] serialize(String s, TopFiveSongs topFiveSongs) {
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    final DataOutputStream dataOutputStream = new DataOutputStream(out);
                    try {
                        for (final SongPlayCount songPlayCount : topFiveSongs) {
                            dataOutputStream.writeLong(songPlayCount.getSongId());
                            dataOutputStream.writeLong(songPlayCount.getPlays());
                        }
                        dataOutputStream.flush();
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                    return out.toByteArray();
                }
            };
        }

        @Override
        public Deserializer<TopFiveSongs> deserializer() {
            return new Deserializer<TopFiveSongs>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {

                }

                @Override
                public TopFiveSongs deserialize(String s, byte[] bytes) {
                    if (bytes == null || bytes.length == 0) {
                        return null;
                    }
                    final TopFiveSongs result = new TopFiveSongs();

                    final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
                    try {
                        while (dataInputStream.available() > 0) {
                            result.add(new SongPlayCount(dataInputStream.readLong(),
                                    dataInputStream.readLong()));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return result;
                }

                @Override
                public void close() {

                }
            };
        }
    }

    static class TopFiveSongs implements Iterable<SongPlayCount> {
        private final Map<Long, SongPlayCount> currentSongs = new HashMap<>();
        private final TreeSet<SongPlayCount> topFive = new TreeSet<>((o1, o2) -> {
            final Long o1Plays = o1.getPlays();
            final Long o2Plays = o2.getPlays();

            final int result = o2Plays.compareTo(o1Plays);
            if (result != 0) {
                return result;
            }
            final Long o1SongId = o1.getSongId();
            final Long o2SongId = o2.getSongId();
            return o1SongId.compareTo(o2SongId);
        });

        @Override
        public String toString() {
            return "TopFiveSongs{" +
                    "currentSongs=" + currentSongs +
                    '}';
        }

        public void add(final SongPlayCount songPlayCount) {
            if (currentSongs.containsKey(songPlayCount.getSongId())) {
                topFive.remove(currentSongs.remove(songPlayCount.getSongId()));
            }
            topFive.add(songPlayCount);
            currentSongs.put(songPlayCount.getSongId(), songPlayCount);
            if (topFive.size() > 5) {
                final SongPlayCount last = topFive.last();
                currentSongs.remove(last.getSongId());
                topFive.remove(last);
            }
        }

        void remove(final SongPlayCount value) {
            topFive.remove(value);
            currentSongs.remove(value.getSongId());
        }


        @Override
        public Iterator<SongPlayCount> iterator() {
            return topFive.iterator();
        }
    }
}
