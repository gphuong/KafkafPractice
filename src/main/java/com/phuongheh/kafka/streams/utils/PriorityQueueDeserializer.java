package com.phuongheh.kafka.streams.utils;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class PriorityQueueDeserializer<T> implements Deserializer<PriorityQueue<T>> {
    private final Comparator<T> comparator;
    private final Deserializer<T> valueDeserializer;

    public PriorityQueueDeserializer(Comparator<T> comparator, Deserializer<T> valueDeserializer) {
        this.comparator = comparator;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public PriorityQueue<T> deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        final PriorityQueue<T> priorityQueue = new PriorityQueue<>(comparator);
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                if (dataInputStream.read(valueBytes) != valueBytes.length) {
                    throw new BufferUnderflowException();
                }
                priorityQueue.add(valueDeserializer.deserialize(s, valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize PriorityQueue", e);
        }
        return priorityQueue;
    }

    @Override
    public void close() {

    }
}
