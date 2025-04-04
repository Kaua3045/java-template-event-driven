package com.kaua.event.driven.infrastructure.es.aggregates.repositories;

import com.kaua.event.driven.domain.exceptions.AggregateDeletionException;
import com.kaua.event.driven.infrastructure.cache.Cache;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateCacheEntry;
import com.kaua.event.driven.infrastructure.es.aggregates.EventSourcedAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.LockFactory;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;

public class CacheEventSourcingRepository<T> extends EventSourcingRepository<T> {

    private final EventStore eventStore;
    private final Cache cache;

    public CacheEventSourcingRepository(
            EventStore eventStore,
            LockFactory lockFactory,
            AggregateModel<T> model,
            Cache cache
    ) {
        super(eventStore, lockFactory, model);
        this.eventStore = eventStore;
        this.cache = cache;
    }

    @Override
    public void validateOnLoad(Aggregate<T> aggregate, Long expectedVersion) {
        CurrentUnitOfWork.get().onRollback(u -> cache.remove(aggregate.identifierAsString()));
        super.validateOnLoad(aggregate, expectedVersion);
    }

    @Override
    public EventSourcedAggregate<T> doLoadWithLock(String aggregateIdentifier) {
        EventSourcedAggregate<T> aggregate = null;
        AggregateCacheEntry<T> cachedAggregate = cache.get(aggregateIdentifier);

        if (cachedAggregate != null) {
            CurrentUnitOfWork.get().onRollback(u -> cache.remove(aggregateIdentifier));
            aggregate = cachedAggregate.recreateAggregate(
                    getAggregateModel(),
                    eventStore
            );
        }

        if (aggregate == null) {
            return super.doLoadWithLock(aggregateIdentifier);
        } else if (aggregate.isDeleted()) {
            throw new AggregateDeletionException(aggregateIdentifier);
        }
        return aggregate;
    }

    @Override
    public EventSourcedAggregate<T> doLoadWithLock(String aggregateIdentifier, Long expectedVersion) {
        EventSourcedAggregate<T> aggregate = null;
        AggregateCacheEntry<T> cachedAggregate = cache.get(aggregateIdentifier);

        if (cachedAggregate != null) {
            CurrentUnitOfWork.get().onRollback(u -> cache.remove(aggregateIdentifier));
            aggregate = cachedAggregate.recreateAggregate(
                    getAggregateModel(),
                    eventStore
            );
        }

        if (aggregate == null) {
            return super.doLoadWithLock(aggregateIdentifier, expectedVersion);
        } else if (aggregate.isDeleted()) {
            throw new AggregateDeletionException(aggregateIdentifier);
        }
        return aggregate;
    }

    @Override
    protected void doSaveWithLock(EventSourcedAggregate<T> aggregate) {
        super.doSaveWithLock(aggregate);
        String key = aggregate.identifierAsString();
        CurrentUnitOfWork.get().onRollback(u -> cache.remove(aggregate.identifierAsString()));
        cache.put(key, new AggregateCacheEntry<>(aggregate));
    }

    @Override
    protected void doDeleteWithLock(EventSourcedAggregate<T> aggregate) {
        super.doDeleteWithLock(aggregate);
        String key = aggregate.identifierAsString();
        CurrentUnitOfWork.get().onRollback(u -> cache.remove(aggregate.identifierAsString()));
        cache.put(key, new AggregateCacheEntry<>(aggregate));
    }
}
