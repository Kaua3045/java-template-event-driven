package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;

import java.io.Serializable;

public class AggregateCacheEntry<T> implements Serializable {

    private final T aggregateRoot;
    private final Long version;
    private final boolean isDeleted;

    private final transient EventSourcedAggregate<T> aggregate;

    public AggregateCacheEntry(EventSourcedAggregate<T> eventSourcedAggregate) {
        this.aggregate = eventSourcedAggregate;
        this.aggregateRoot = eventSourcedAggregate.getAggregateRoot();
        this.version = eventSourcedAggregate.version();
        this.isDeleted = eventSourcedAggregate.isDeleted();
    }

    public EventSourcedAggregate<T> recreateAggregate(
            AggregateModel<T> model,
            EventStore eventStore
    ) {
        if (aggregate != null) {
            return aggregate;
        }

        return new EventSourcedAggregate<>(
                model,
                eventStore,
                aggregateRoot
        );
    }
}
