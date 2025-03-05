package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class NoHandlerForEventExceptionTest extends UnitTest {

    @Test
    void shouldCreateNoHandlerForEventException() {
        var event = new EventStub();
        var exception = new NoHandlerForEventException(event);
        Assertions.assertEquals("No matching handler available to handle event [%s]"
                        .formatted(event.getClass().getSimpleName()),
                exception.getMessage()
        );
    }

    private record EventStub(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId
    ) implements DomainEvent {

        public EventStub() {
            this(
                    "eventId",
                    "eventType",
                    Instant.now(),
                    "aggregateId",
                    1L,
                    "source",
                    "traceId"
            );
        }
    }
}
