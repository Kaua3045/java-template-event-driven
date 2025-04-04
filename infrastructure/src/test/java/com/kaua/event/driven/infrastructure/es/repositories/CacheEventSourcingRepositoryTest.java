package com.kaua.event.driven.infrastructure.es.repositories;

import com.kaua.event.driven.utils.InMemoryEventStore;
import com.kaua.event.driven.utils.StubAggregate;
import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.AggregateDeletionException;
import com.kaua.event.driven.infrastructure.cache.Cache;
import com.kaua.event.driven.infrastructure.cache.InMemoryCache;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.EventSourcedAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.LockAwareAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.CacheEventSourcingRepository;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.PessimisticLockFactory;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.DefaultUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheEventSourcingRepositoryTest extends UnitTest {

    private CacheEventSourcingRepository<StubAggregate> testSubject;
    private EventStore mockEventStore;
    private Cache cache;
    private InMemoryCache inMemoryCache;
    private AggregateModel<StubAggregate> aggregateModel;

    @BeforeEach
    void setUp() {
        mockEventStore = spy(new InMemoryEventStore());
        inMemoryCache = new InMemoryCache();
        cache = spy(inMemoryCache);
        aggregateModel = new DefaultAggregateModel<>(StubAggregate.class);

        testSubject = new CacheEventSourcingRepository<>(
                mockEventStore,
                new PessimisticLockFactory(
                        10,
                        10,
                        10
                ),
                aggregateModel,
                cache
        );
    }

    @AfterEach
    void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @Test
    void aggregatesRetrievedFromCache() throws Exception {
        startAndGetUnitOfWork();

        LockAwareAggregate<StubAggregate, EventSourcedAggregate<StubAggregate>> aggregate1 =
                (LockAwareAggregate<StubAggregate, EventSourcedAggregate<StubAggregate>>) testSubject.newInstance(() -> new StubAggregate("aggregateId"));

        aggregate1.execute(StubAggregate::doSomething);
        assertEquals(1, aggregate1.getWrappedAggregate().version());
        CurrentUnitOfWork.commit();

        startAndGetUnitOfWork();
        LockAwareAggregate<StubAggregate, EventSourcedAggregate<StubAggregate>> reloadedAggregate1 =
                (LockAwareAggregate<StubAggregate, EventSourcedAggregate<StubAggregate>>) testSubject.load("aggregateId", null);
        assertEquals(1, reloadedAggregate1.getWrappedAggregate().version());
        aggregate1.execute(StubAggregate::doSomething);
        aggregate1.execute(StubAggregate::doSomething);
        assertEquals(3, aggregate1.getWrappedAggregate().version());
        CurrentUnitOfWork.commit();

        DefaultUnitOfWork.startAndGet(null);
        List<DomainEvent> events = mockEventStore.readEvents("aggregateId");
        List<DomainEvent> eventList = new ArrayList<>(events);

        assertEquals(3, eventList.size());
        inMemoryCache.clear();

        reloadedAggregate1 = (LockAwareAggregate<StubAggregate, EventSourcedAggregate<StubAggregate>>) testSubject.load(aggregate1.identifierAsString(), null);

        assertNotSame(aggregate1.getWrappedAggregate(), reloadedAggregate1.getWrappedAggregate());
        assertEquals(aggregate1.version(), reloadedAggregate1.version());
        assertEquals(3, reloadedAggregate1.getWrappedAggregate().version());
    }

    @Test
    void loadOrCreateNewAggregate() {
        startAndGetUnitOfWork();
        Aggregate<StubAggregate> aggregate = testSubject.loadOrCreate("id1", StubAggregate::new);
        aggregate.execute(s -> s.setId("id1"));

        CurrentUnitOfWork.commit();

        assertNotNull(cache.get("id1"));
        verify(cache, never()).put(isNull(), any());
    }

    @Test
    void loadDeletedAggregate() throws Exception {
        String identifier = "aggregateId";

        startAndGetUnitOfWork();
        Aggregate<StubAggregate> aggregate1 = testSubject.newInstance(() -> new StubAggregate(identifier));
        CurrentUnitOfWork.commit();

        startAndGetUnitOfWork();
        testSubject.load(identifier).execute((r) -> AggregateLifecycle.markDeleted());
        CurrentUnitOfWork.commit();

        startAndGetUnitOfWork();
        try {
            testSubject.load(identifier);
            fail("Expected AggregateDeletionException");
        } catch (AggregateDeletionException e) {
            assertTrue(e.getMessage().contains(identifier));
        } finally {
            CurrentUnitOfWork.commit();
        }
    }

    @Test
    void cacheClearedAfterRollbackOfAddedAggregate() throws Exception {
        UnitOfWork<?> uow = startAndGetUnitOfWork();
        uow.onCommit(c -> {
            throw new RuntimeException();
        });
        try {
            testSubject.newInstance(() -> new StubAggregate("id1")).execute(StubAggregate::doSomething);
            uow.commit();
        } catch (RuntimeException e) {
            // great, that's what we expect
        }
        assertNull(cache.get("id1"));
    }

    @Test
    void cacheClearedAfterRollbackOfLoadedAggregate() {

        startAndGetUnitOfWork().executeWithResult(() -> testSubject.newInstance(() -> new StubAggregate("id1")));

        UnitOfWork<?> uow = startAndGetUnitOfWork();
        uow.onCommit(c -> {
            throw new RuntimeException();
        });
        try {
            testSubject.load("id1").execute(StubAggregate::doSomething);
            uow.commit();
        } catch (RuntimeException e) {
            // great, that's what we expect
        }
        assertNull(cache.get("id1"));
    }

    @Test
    void cacheClearedAfterRollbackOfLoadedAggregateUsingLoadOrCreate() throws Exception {

        startAndGetUnitOfWork().executeWithResult(() -> testSubject.newInstance(() -> new StubAggregate("id1")));

        UnitOfWork<?> uow = startAndGetUnitOfWork();
        uow.onCommit(c -> {
            throw new RuntimeException();
        });
        try {
            testSubject.loadOrCreate("id1", () -> new StubAggregate("id1")).execute(StubAggregate::doSomething);
            uow.commit();
        } catch (RuntimeException e) {
            // great, that's what we expect
        }
        assertNull(cache.get("id1"));
    }

    @Test
    void cacheClearedAfterRollbackOfCreatedAggregateUsingLoadOrCreate() throws Exception {

        UnitOfWork<?> uow = startAndGetUnitOfWork();
        uow.onCommit(c -> {
            throw new RuntimeException();
        });
        try {
            testSubject.loadOrCreate("id1", () -> new StubAggregate("id1")).execute(StubAggregate::doSomething);
            uow.commit();
        } catch (RuntimeException e) {
            // great, that's what we expect
        }
        assertNull(cache.get("id1"));
    }

    private UnitOfWork<?> startAndGetUnitOfWork() {
        return DefaultUnitOfWork.startAndGet(null);
    }
}
