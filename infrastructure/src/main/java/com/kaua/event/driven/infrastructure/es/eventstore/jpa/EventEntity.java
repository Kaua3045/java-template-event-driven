package com.kaua.event.driven.infrastructure.es.eventstore.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "events") // TODO this class need refactor
public class EventEntity {

    @Id
    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "occurred_on", nullable = false)
    private Instant occurredOn;

    @Column(name = "aggregate_version", nullable = false)
    private long aggregateVersion;

    public EventEntity() {
    }

    public EventEntity(String eventId, String aggregateId, String eventType, String payload, Instant occurredOn, long aggregateVersion) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.occurredOn = occurredOn;
        this.aggregateVersion = aggregateVersion;
    }

    public static EventEntity toEntity(final DomainEvent event) {
        return new EventEntity(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                Json.writeValueAsString(event), // TODO check this, to serialize in byte[] or String
                event.occurredOn(),
                event.aggregateVersion()
        );
    }

    public DomainEvent toDomain() {
        try {
            final var aClazz = Class.forName(getEventType());
            return (DomainEvent) Json.readValue(getPayload(), aClazz);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getOccurredOn() {
        return occurredOn;
    }

    public void setOccurredOn(Instant occurredOn) {
        this.occurredOn = occurredOn;
    }

    public long getAggregateVersion() {
        return aggregateVersion;
    }

    public void setAggregateVersion(long aggregateVersion) {
        this.aggregateVersion = aggregateVersion;
    }
}
