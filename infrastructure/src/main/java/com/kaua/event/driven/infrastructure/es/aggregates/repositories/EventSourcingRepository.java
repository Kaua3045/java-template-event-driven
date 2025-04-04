package com.kaua.event.driven.infrastructure.es.aggregates.repositories;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.AggregateDeletionException;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.EventSourcedAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.conflict.DefaultConflictResolver;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

public class EventSourcingRepository<T> extends LockingRepository<T, EventSourcedAggregate<T>> {

    private static final Logger log = LoggerFactory.getLogger(EventSourcingRepository.class);

    private final EventStore eventStore;
    private final AggregateModel<T> model;

    public EventSourcingRepository(
            EventStore eventStore,
            LockFactory lockFactory,
            AggregateModel<T> model
    ) {
        super(model, lockFactory);
        this.eventStore = eventStore;
        this.model = model;
    }

    @Override
    public EventSourcedAggregate<T> doCreateNewForLock(Callable<T> factoryMethod) throws Exception {
        log.debug("Event sourced aggregate created");
        return new EventSourcedAggregate<>(model, eventStore, factoryMethod.call());
    }

    @Override
    public EventSourcedAggregate<T> doLoadWithLock(String aggregateIdentifier) {
        final var aEvents = readEvents(aggregateIdentifier);
        if (aEvents.isEmpty()) {
            throw NotFoundException.withIdentifier(aggregateIdentifier);
        }

        final var aAggregate = reconstructAggregate(aEvents);

        if (aAggregate.isDeleted()) {
            throw new AggregateDeletionException(aggregateIdentifier);
        }

        log.info("Aggregate loaded: {}", aggregateIdentifier);

        return aAggregate;
    }

    @Override
    public EventSourcedAggregate<T> doLoadWithLock(String aggregateIdentifier, Long expectedVersion) {
        final var aEvents = readEvents(aggregateIdentifier);
        if (aEvents.isEmpty()) {
            throw NotFoundException.withIdentifierAndVersion(aggregateIdentifier, expectedVersion);
        }

        final var aAggregate = reconstructAggregate(aEvents);


        if (aAggregate.isDeleted()) {
            throw new AggregateDeletionException(aggregateIdentifier);
        }

        log.info("Aggregate loaded with version, identifier: {}, version: {}", aggregateIdentifier, expectedVersion);

        return aAggregate;
    }

    @Override
    protected void doSaveWithLock(EventSourcedAggregate<T> aggregate) {
        // no-op
    }

    @Override
    protected void doDeleteWithLock(EventSourcedAggregate<T> aggregate) {
        // no-op
    }

    public List<DomainEvent> readEvents(String aggregateIdentifier) {
        return eventStore.readEvents(aggregateIdentifier);
    }

    @Override
    public void validateOnLoad(Aggregate<T> aggregate, Long expectedVersion) {
        if (expectedVersion != null && expectedVersion < aggregate.version()) {
            DefaultConflictResolver conflictResolver = new DefaultConflictResolver(
                    eventStore,
                    aggregate.identifierAsString(),
                    expectedVersion,
                    aggregate.version()
            );
            conflictResolver.ensureConflictsResolved();
        } else {
            super.validateOnLoad(aggregate, expectedVersion);
        }
    }

    private EventSourcedAggregate<T> reconstructAggregate(List<DomainEvent> aEvents) {
        try {
            final var aSortedEvents = new ArrayList<>(aEvents);
            aSortedEvents.sort(Comparator.comparingLong(DomainEvent::aggregateVersion));

            final var aAggregateSourced = new EventSourcedAggregate<>(model, eventStore, newInstance(model.entityClass()));

            aEvents.forEach(aAggregateSourced::handle);

            return aAggregateSourced;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T newInstance(Class<?> type) {
        try {
            return (T) type.getDeclaredConstructor().newInstance();
        } catch (final InstantiationException | NoSuchMethodException e) {
            throw new InternalErrorException("The aggregate does not have a suitable no-arg constructor", e);
        } catch (final IllegalAccessException e) {
            throw new InternalErrorException("The JVM security settings prevent calling the constructor", e);
        } catch (final InvocationTargetException e) {
            throw new InternalErrorException("The aggregate constructor throws an exception", e);
        }
    }
}
