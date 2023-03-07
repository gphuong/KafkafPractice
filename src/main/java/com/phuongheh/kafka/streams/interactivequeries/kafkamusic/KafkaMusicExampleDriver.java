package com.phuongheh.kafka.streams.interactivequeries.kafkamusic;

import com.phuongheh.kafka.streams.avro.PlayEvent;
import com.phuongheh.kafka.streams.avro.Song;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class KafkaMusicExampleDriver {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);

        final List<Song> songs = new ArrayList<>();
        final String SONGFILENAME = "song_source.csv";
        final InputStream inputStream = KafkaMusicExample.class.getClassLoader().getResourceAsStream(SONGFILENAME);
        final InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        try (final BufferedReader br = new BufferedReader(streamReader)) {
            String line = null;
            while ((line = br.readLine()) != null) {
                final String[] values = line.split(",");
                final Song newSong = new Song(Long.parseLong(values[0]), values[1], values[2], values[3], values[4]);
                songs.add(newSong);
            }
        }
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<PlayEvent> playEventSerializer = new SpecificAvroSerializer<>();
        playEventSerializer.configure(serdeConfig, false);
        final SpecificAvroSerializer<Song> songSerializer = new SpecificAvroSerializer<>();
        songSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, PlayEvent> playEventProducer= new KafkaProducer<String, PlayEvent>(props, Serdes.String().serializer(), playEventSerializer);
        final KafkaProducer<Long, Song> songProducer = new KafkaProducer<Long, Song>(props, new LongSerializer(), songSerializer);

        songs.forEach(song -> {
            System.out.println("Writing song information for '"+song.getName() + "' to input topic"+ KafkaMusicExample.SONG_FEED);
            songProducer.send(new ProducerRecord<>(KafkaMusicExample.SONG_FEED, song.getId(), song));
        });

        songProducer.close();
        final long duration = 60 * 1000L;
        final Random random = new Random();

        while(true){
            final Song song = songs.get(random.nextInt(songs.size()));
            System.out.println("Writing play event for song "+ song.getName() + " to input topic "+ KafkaMusicExample.PLAY_EVENTS);
            playEventProducer.send(new ProducerRecord<>(KafkaMusicExample.PLAY_EVENTS, "uk", new PlayEvent(song.getId(), duration)));
            Thread.sleep(100L);
        }
    }
}
