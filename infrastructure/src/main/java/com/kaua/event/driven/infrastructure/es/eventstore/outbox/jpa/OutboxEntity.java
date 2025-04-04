package com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStatus;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
public class OutboxEntity {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload; // TODO check this, to store in byte[] or String json format

    @Column(name = "occurred_on", nullable = false)
    private Instant occurredOn;

    @Column(name = "aggregate_version", nullable = false)
    private long aggregateVersion;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxStatus status;

    public OutboxEntity() {
    }

    public OutboxEntity(
            final String eventId,
            final String aggregateId,
            final String eventType,
            final String payload,
            final Instant occurredOn,
            final long aggregateVersion,
            final OutboxStatus status
    ) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.occurredOn = occurredOn;
        this.aggregateVersion = aggregateVersion;
        this.status = status;
    }

    public static OutboxEntity toEntity(final DomainEvent event) {
        return new OutboxEntity(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                Json.writeValueAsString(event), // TODO check this, to serialize in byte[] or String
                event.occurredOn(),
                event.aggregateVersion(),
                OutboxStatus.PENDING
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

    public OutboxStatus getStatus() {
        return status;
    }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }
}
