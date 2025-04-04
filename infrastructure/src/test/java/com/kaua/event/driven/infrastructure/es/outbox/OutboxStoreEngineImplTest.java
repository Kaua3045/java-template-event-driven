package com.kaua.event.driven.infrastructure.es.outbox;

import com.kaua.event.driven.DatabaseGatewayTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStatus;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.JpaOutboxStoreEngine;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.OutboxEntity;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.OutboxJpaRepository;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@DatabaseGatewayTest
class OutboxStoreEngineImplTest {

    private JpaOutboxStoreEngine jpaOutboxStoreEngine;

    @Autowired
    private OutboxJpaRepository outboxJpaRepository;

    @BeforeEach
    void setup() {
        jpaOutboxStoreEngine = new JpaOutboxStoreEngine(outboxJpaRepository);
    }

    @Test
    void testCount() {
        Assertions.assertEquals(0, outboxJpaRepository.count());
    }

    @Test
    void givenAValidSingleDomainEvent_whenCallStore_thenShouldStoreEvent() {
        final var aDomainEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        jpaOutboxStoreEngine.store(aDomainEvent);

        Assertions.assertEquals(1, outboxJpaRepository.count());

        final var aEntity = outboxJpaRepository.findById(aDomainEvent.eventId()).get();

        Assertions.assertEquals(aDomainEvent.eventId(), aEntity.getEventId());
        Assertions.assertEquals(aDomainEvent.aggregateId(), aEntity.getAggregateId());
        Assertions.assertEquals(aDomainEvent.eventType(), aEntity.getEventType());
        Assertions.assertEquals(aDomainEvent.occurredOn(), aEntity.getOccurredOn());
        Assertions.assertEquals(aDomainEvent.aggregateVersion(), aEntity.getAggregateVersion());
    }

    @Test
    void givenAValidListOfDomainEvents_whenCallStore_thenShouldStoreEvents() {
        final var aDomainEvent1 = new StubDomainEvent(IdentifierUtils.generateNewId(), "test1");
        final var aDomainEvent2 = new StubDomainEvent(IdentifierUtils.generateNewId(), "test2");

        jpaOutboxStoreEngine.store(List.of(aDomainEvent1, aDomainEvent2));

        Assertions.assertEquals(2, outboxJpaRepository.count());
    }

    @Test
    void givenAValid51Events_whenCallFindTop50ByStatusOrderByOccurredOnAsc_thenReturn50Events() {
        for (int i = 0; i < 50; i++) {
            final var aDomainEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test" + i);
            jpaOutboxStoreEngine.store(aDomainEvent);
            System.out.println("Event " + i + " stored");
        }

        final var aFailedDomainEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "testFailed");
        final var aEntity = OutboxEntity.toEntity(aFailedDomainEvent);
        aEntity.setStatus(OutboxStatus.FAILED);
        outboxJpaRepository.save(aEntity);

        Assertions.assertEquals(51, outboxJpaRepository.count());

        final var events = jpaOutboxStoreEngine.findTop50ByStatusOrderByOccurredOnAsc(
                OutboxStatus.PENDING
        );

        Assertions.assertEquals(50, events.size());

        final var aEntityFailed = outboxJpaRepository.findById(aFailedDomainEvent.eventId()).get();

        Assertions.assertEquals(OutboxStatus.FAILED, aEntityFailed.getStatus());

        Assertions.assertFalse(events.stream().anyMatch(it -> it.eventId().equals(aFailedDomainEvent.eventId())));
    }

    @Test
    void givenAnInvalidEvent_whenCallToDomain_thenThrowException() {
        final var aDomainEvent = new EventDomain(IdentifierUtils.generateNewId(), "test");

        outboxJpaRepository.save(OutboxEntity.toEntity(aDomainEvent));

        final var aResult = outboxJpaRepository.findById(aDomainEvent.eventId()).get();

        final var aException = Assertions.assertThrows(
                RuntimeException.class,
                aResult::toDomain
        );

        Assertions.assertEquals("java.lang.ClassNotFoundException: eventDomain", aException.getMessage());
    }

    @Test
    void givenAValidEventButSetValuesInEntity_whenCallSaveInRepository_thenSaveEvent() {
        final var aDomainEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        final var aEntity = OutboxEntity.toEntity(aDomainEvent);
        aEntity.setEventId(aDomainEvent.eventId());
        aEntity.setAggregateId(aDomainEvent.aggregateId());
        aEntity.setEventType(aDomainEvent.eventType());
        aEntity.setOccurredOn(aDomainEvent.occurredOn());
        aEntity.setAggregateVersion(aDomainEvent.aggregateVersion());
        aEntity.setPayload(Json.writeValueAsString(aDomainEvent));

        outboxJpaRepository.save(aEntity);

        final var aResult = outboxJpaRepository.findById(aDomainEvent.eventId()).get();

        Assertions.assertEquals(aDomainEvent.eventId(), aResult.getEventId());
        Assertions.assertEquals(aDomainEvent.aggregateId(), aResult.getAggregateId());
        Assertions.assertEquals(aDomainEvent.eventType(), aResult.getEventType());
        Assertions.assertEquals(aDomainEvent.occurredOn(), aResult.getOccurredOn());
        Assertions.assertEquals(aDomainEvent.aggregateVersion(), aResult.getAggregateVersion());
    }

    public record EventDomain(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId,
            String text
    ) implements DomainEvent {

        public EventDomain(String aggregateId, String text) {
            this(UUID.randomUUID().toString(), "eventDomain", Instant.now(), aggregateId, 0, "DomainEventTest", UUID.randomUUID().toString(), text);
        }
    }
}
