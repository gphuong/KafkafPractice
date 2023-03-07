package com.phuongheh.kafka.streams.interactivequeries;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.StreamsMetadata;

import javax.ws.rs.NotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataService {
    private final KafkaStreams streams;

    public MetadataService(KafkaStreams streams) {
        this.streams = streams;
    }

    public List<HostStoreInfo> streamsMetadata() {
        final Collection<StreamsMetadata> metadata = streams.allMetadata();
        return mapInstanceToHostStoreInfo(metadata);
    }

    public List<HostStoreInfo> streamsMetadataForStore(final String store) {
        final Collection<StreamsMetadata> metadata = streams.allMetadataForStore(store);
        return mapInstanceToHostStoreInfo(metadata);
    }

    public <K> HostStoreInfo streamsMetadataForStoreAndKey(final String store,
                                                           final K key,
                                                           final Serializer<K> serializer) {
        final KeyQueryMetadata metadata = streams.queryMetadataForKey(store, key, serializer);
        if (metadata == null)
            throw new NotFoundException();
        return new HostStoreInfo(metadata.activeHost().host(),
                metadata.activeHost().port(),
                Collections.singleton(store));
    }

    private List<HostStoreInfo> mapInstanceToHostStoreInfo(Collection<StreamsMetadata> metadatas) {
        return metadatas.stream().map(metadata -> new HostStoreInfo(metadata.host(), metadata.port(), metadata.stateStoreNames()))
                .collect(Collectors.toList());
    }
}
