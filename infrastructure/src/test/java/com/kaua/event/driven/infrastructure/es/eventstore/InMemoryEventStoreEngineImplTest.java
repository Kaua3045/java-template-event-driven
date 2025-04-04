package com.kaua.event.driven.infrastructure.es.eventstore;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.eventstore.memory.InMemoryEventStoreEngine;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class InMemoryEventStoreEngineImplTest extends UnitTest {

    @Test
    void givenAValidSingleDomainEvent_whenCallStore_thenShouldStoreEvent() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();

        final var aEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(aEvent));
    }

    @Test
    void givenAValidMoreEventOnContainsAggregate_whenCallStore_thenShouldStoreEvents() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();

        final var aId = IdentifierUtils.generateNewId();
        final var aEvent = new StubDomainEvent(aId, "test");
        final var aEvent2 = new StubDomainEvent(aId, "test2");

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(aEvent));
        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(aEvent2));
    }

    @Test
    void givenAnInvalidNullDomainEvent_whenCallStore_thenShouldThrowException() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();
        final var expectedErrorMessage = "DomainEvent cannot be null";

        final var aException = Assertions.assertThrows(InternalErrorException.class, () -> aInMemoryEngine.store(
                (DomainEvent) null
        ));

        Assertions.assertEquals(expectedErrorMessage, aException.getMessage());
    }

    @Test
    void givenAnInvalidEmptyDomainEvent_whenCallStore_thenShouldThrowException() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();
        final var expectedErrorMessage = "DomainEvents cannot be empty";

        final var aException = Assertions.assertThrows(InternalErrorException.class, () -> aInMemoryEngine.store(
                Collections.emptyList()
        ));

        Assertions.assertEquals(expectedErrorMessage, aException.getMessage());
    }

    @Test
    void givenACollectionOfDomainEvents_whenCallStore_thenShouldStoreEvents() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();

        final var aId = IdentifierUtils.generateNewId();
        final var aEvent = new StubDomainEvent(aId, "test");
        final var aEvent2 = new StubDomainEvent(aId, "test2");

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(
                List.of(aEvent, aEvent2)
        ));
    }

    @Test
    void givenAnAggregateExistsAndStoreCollectionOfDomainEvents_whenCallReadEvents_thenShouldStoreEvents() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();

        final var aId = IdentifierUtils.generateNewId();
        final var aEvent = new StubDomainEvent(aId, "test");
        final var aEvent2 = new StubDomainEvent(aId, "test2");

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(
                List.of(aEvent)
        ));

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(
                List.of(aEvent2)
        ));
    }

    @Test
    void givenAnAggregateIdExists_whenCallReadEvents_thenShouldReturnEvents() {
        final var aInMemoryEngine = new InMemoryEventStoreEngine();

        final var aId = IdentifierUtils.generateNewId();
        final var aEvent = new StubDomainEvent(aId, "test");
        final var aEvent2 = new StubDomainEvent(aId, "test2");

        Assertions.assertDoesNotThrow(() -> aInMemoryEngine.store(
                List.of(aEvent, aEvent2)
        ));

        final var aEvents = aInMemoryEngine.readEvents(aId);

        Assertions.assertEquals(2, aEvents.size());
    }
}
