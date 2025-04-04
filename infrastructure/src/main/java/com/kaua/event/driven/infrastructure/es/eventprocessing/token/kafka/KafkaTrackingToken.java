package com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;

public class KafkaTrackingToken implements TrackingToken {

    private final String processorName;
    private final Instant occurredOn;
    private final long offset;

    @JsonDeserialize(keyUsing = TopicPartitionKeyDeserializer.class)
    private final Map<TopicPartition, Long> positions;

    @JsonCreator
    public KafkaTrackingToken(
            @JsonProperty("processor_name") String processorName,
            @JsonProperty("occurred_on") Instant occurredOn,
            @JsonProperty("offset") long offset,
            @JsonProperty("positions") Map<TopicPartition, Long> positions
    ) {
        this.processorName = processorName;
        this.occurredOn = occurredOn;
        this.offset = offset;
        this.positions = positions;
    }

    @Override
    public String processorName() {
        return processorName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public Instant getOccurredOn() {
        return occurredOn;
    }

    public long getOffset() {
        return offset;
    }

    public Map<TopicPartition, Long> getPositions() {
        return positions;
    }

    @Override
    public String toString() {
        return "KafkaTrackingToken(" +
                "processorName='" + processorName + '\'' +
                ", occurredOn=" + occurredOn +
                ", offset=" + offset +
                ", positions=" + positions.size() +
                ')';
    }
}
