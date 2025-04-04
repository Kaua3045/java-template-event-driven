package com.kaua.event.driven.utils;

import com.kaua.event.driven.domain.events.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record StubDomainEvent(
        String eventId,
        String eventType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String source,
        String traceId,
        String text
) implements DomainEvent {

    public StubDomainEvent(String aggregateId, String text) {
        this(UUID.randomUUID().toString(), StubDomainEvent.class.getName(), Instant.now(), aggregateId, 0, "DomainEventTest", UUID.randomUUID().toString(), text);
    }

    public StubDomainEvent(String aggregateId, long aggregateVersion, String text) {
        this(UUID.randomUUID().toString(), StubDomainEvent.class.getName(), Instant.now(), aggregateId, aggregateVersion, "DomainEventTest", UUID.randomUUID().toString(), text);
    }
}
