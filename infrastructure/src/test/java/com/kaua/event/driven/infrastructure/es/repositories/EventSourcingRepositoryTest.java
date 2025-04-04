package com.kaua.event.driven.infrastructure.es.repositories;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.ConflictAggregateVersionException;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateRoot;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateVersion;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventSourcingHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.TargetAggregateIdentifier;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.EventSourcingRepository;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.NullLockFactory;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.DefaultUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventSourcingRepositoryTest extends UnitTest {

    private EventStore mockEventStore;
    private EventSourcingRepository<TestAggregate> testSubject;
    private UnitOfWork<?> unitOfWork;
    private AggregateModel<TestAggregate> aggregateModel;

    @BeforeEach
    void setUp() {
        mockEventStore = mock(EventStore.class);
        aggregateModel = new DefaultAggregateModel<>(TestAggregate.class);
        testSubject = new EventSourcingRepository<>(
                mockEventStore,
                NullLockFactory.INSTANCE,
                aggregateModel
        );
        unitOfWork = DefaultUnitOfWork.startAndGet(
                new DomainEventTest(IdentifierUtils.generateNewId(), "test"));
    }

    @AfterEach
    void tearDown() {
        if (unitOfWork.isActive()) {
            unitOfWork.rollback();
        }
    }

    @Test
    void loadAndSaveAggregate() {
        String identifier = UUID.randomUUID().toString();
        DomainEvent event1 = new DomainEventTest(identifier, "Mock contents 1");
        DomainEvent event2 = new DomainEventTest(identifier, "Mock contents 2");
        when(mockEventStore.readEvents(identifier)).thenReturn(List.of(event1, event2));

        Aggregate<TestAggregate> aggregate = testSubject.load(identifier);

        assertEquals(2, aggregate.invoke(TestAggregate::getHandledEvents).size());
        assertSame(event1, aggregate.invoke(TestAggregate::getHandledEvents).get(0));
        assertSame(event2, aggregate.invoke(TestAggregate::getHandledEvents).get(1));

        assertEquals(0, aggregate.invoke(TestAggregate::getLiveEvents).size());

        // now the aggregate is loaded (and hopefully correctly locked)
        DomainEvent event3 = new DomainEventTest(identifier, "Mock contents 3");

        aggregate.execute(r -> r.apply(event3));

        CurrentUnitOfWork.commit();

        verify(mockEventStore, times(1)).publish(any());
//        assertEquals(1, aggregate.invoke(TestAggregate::getLiveEvents).size());
//        assertSame(event3, aggregate.invoke(TestAggregate::getLiveEvents).get(0));
    }

    @Test
    void loadWithConflictingChanges() {
        String identifier = UUID.randomUUID().toString();
        when(mockEventStore.readEvents(identifier)).thenReturn(List.of(
                new DomainEventTest(identifier, 1, "Mock contents 1"),
                new DomainEventTest(identifier, 2, "Mock contents 2"),
                new DomainEventTest(identifier, 3, "Mock contents 3")
        ));

        try {
            testSubject.load(identifier, 1L);
            CurrentUnitOfWork.commit();
            fail("Expected ConflictAggregateVersionException");
        } catch (ConflictAggregateVersionException e) {
            assertEquals(identifier, e.getAggregateId());
            assertEquals(1L, e.getExpectedVersion());
            assertEquals(3L, e.getActualVersion());
        }
    }

    @AggregateRoot
    public static class TestAggregate {

        private List<DomainEvent> handledEvents = new ArrayList<>();
        private List<DomainEvent> liveEvents = new ArrayList<>();

        @TargetAggregateIdentifier
        private String identifier;

        @AggregateVersion
        private long version;

        public TestAggregate() {
        }

        private TestAggregate(String identifier, long version) {
            this.identifier = identifier;
            this.version = version;
        }

        public void apply(Object eventPayload) {
            AggregateLifecycle.apply(eventPayload);
        }

        public void changeState() {
            AggregateLifecycle.apply("Test more");
        }

        @EventSourcingHandler
        public void handle(DomainEventTest event) {
            identifier = event.aggregateId();
            version = event.aggregateVersion();
            handledEvents.add(event);
//            if (AggregateLifecycle.isLive()) {
//                liveEvents.add(event);
//            }
        }

        public List<DomainEvent> getHandledEvents() {
            return handledEvents;
        }

        public List<DomainEvent> getLiveEvents() {
            return liveEvents;
        }

        public String getIdentifier() {
            return identifier;
        }

        public long getVersion() {
            return version;
        }
    }

    public record DomainEventTest(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId,
            String text
    ) implements DomainEvent {

        public DomainEventTest(String aggregateId, String text) {
            this(UUID.randomUUID().toString(), "DomainEventTest", Instant.now(), aggregateId, 1, "DomainEventTest", UUID.randomUUID().toString(), text);
        }

        public DomainEventTest(String aggregateId, long aggregateVersion, String text) {
            this(UUID.randomUUID().toString(), "DomainEventTest", Instant.now(), aggregateId, aggregateVersion, "DomainEventTest", UUID.randomUUID().toString(), text);
        }
    }
}
