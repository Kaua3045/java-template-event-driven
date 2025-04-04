package com.kaua.event.driven.infrastructure.es.tests.values;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;

import java.time.Instant;

public record PaymentCreatedEvent(
        String eventId,
        String eventType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String source,
        String traceId,
        String text
) implements DomainEvent {

    public PaymentCreatedEvent(String aggregateId, String text, long aggregateVersion) {
        this(
                IdentifierUtils.generateNewId(),
                PaymentCreatedEvent.class.getName(),
                Instant.now(),
                aggregateId,
                aggregateVersion,
                "OrderService",
                "traceId",
                text
        );
    }
}
