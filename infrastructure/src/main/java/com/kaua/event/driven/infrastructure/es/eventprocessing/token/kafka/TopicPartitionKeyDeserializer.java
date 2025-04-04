package com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka;

import com.fasterxml.jackson.databind.KeyDeserializer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

public class TopicPartitionKeyDeserializer extends KeyDeserializer {

    @Override
    public Object deserializeKey(String key, com.fasterxml.jackson.databind.DeserializationContext ctxt) throws IOException {
        String[] parts;

        if (key.contains(":")) {
            parts = key.split(":");
        } else if (key.contains("-")) {
            int lastHyphenIndex = key.lastIndexOf("-");
            parts = new String[]{key.substring(0, lastHyphenIndex), key.substring(lastHyphenIndex + 1)};
        } else {
            throw new IllegalArgumentException("Invalid TopicPartition key format: " + key);
        }

        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid TopicPartition key format: " + key);
        }

        try {
            int partition = Integer.parseInt(parts[1]);
            return new TopicPartition(parts[0], partition);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid partition number in key: " + key, e);
        }
//        String[] parts = key.split(":");
//        if (parts.length != 2) {
//            throw new IllegalArgumentException("Invalid TopicPartition key format: " + key);
//        }
//        return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
    }
}
