package com.kaua.event.driven.infrastructure.es.tests.values;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;

import java.time.Instant;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String source,
        String traceId,
        String description
) implements DomainEvent {

    public OrderCreatedEvent(String aggregateId, String description, long aggregateVersion) {
        this(
                IdentifierUtils.generateNewId(),
                OrderCreatedEvent.class.getName(),
                Instant.now(),
                aggregateId,
                aggregateVersion,
                "OrderService",
                "traceId",
                description
        );
    }
}
