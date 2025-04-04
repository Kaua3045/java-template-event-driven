package com.kaua.event.driven.infrastructure.es.eventstore;

import com.kaua.event.driven.DatabaseGatewayTest;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.eventstore.jpa.EventJpaRepository;
import com.kaua.event.driven.infrastructure.es.eventstore.jpa.JpaEventStoreEngine;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@DatabaseGatewayTest
class JpaEventStoreEngineImplTest {

    private JpaEventStoreEngine jpaEventStoreEngine;

    @Autowired
    private EventJpaRepository eventJpaRepository;

    @BeforeEach
    void setup() {
        jpaEventStoreEngine = new JpaEventStoreEngine(eventJpaRepository);
    }

    @Test
    void testCount() {
        Assertions.assertEquals(0, eventJpaRepository.count());
    }

    @Test
    void givenAValidSingleDomainEvent_whenCallStore_thenShouldStoreEvent() {
        final var aDomainEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        jpaEventStoreEngine.store(aDomainEvent);

        Assertions.assertEquals(1, eventJpaRepository.count());

        final var aEntity = eventJpaRepository.findById(aDomainEvent.eventId()).get();

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

        jpaEventStoreEngine.store(List.of(aDomainEvent1, aDomainEvent2));

        Assertions.assertEquals(2, eventJpaRepository.count());
    }

    @Test
    void givenAValidAggregateId_whenCallReadEvents_thenShouldReturnEvents() {
        final var aId = IdentifierUtils.generateNewId();
        final var aDomainEvent1 = new StubDomainEvent(aId, "test1");
        final var aDomainEvent2 = new StubDomainEvent(aId, "test2");

        jpaEventStoreEngine.store(List.of(aDomainEvent1, aDomainEvent2));

        final var events = jpaEventStoreEngine.readEvents(aId);

        Assertions.assertEquals(2, events.size());
    }
}
