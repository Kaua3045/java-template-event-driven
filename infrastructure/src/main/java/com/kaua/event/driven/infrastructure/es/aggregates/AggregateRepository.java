package com.kaua.event.driven.infrastructure.es.aggregates;

import java.util.concurrent.Callable;

public interface AggregateRepository<T> {

    Aggregate<T> load(String aggregateIdentifier);

    Aggregate<T> load(String aggregateIdentifier, Long expectedVersion);

    Aggregate<T> newInstance(Callable<T> factoryMethod) throws Exception;

    default Aggregate<T> loadOrCreate(String aggregateIdentifier, Callable<T> factoryMethod) throws Exception {
        throw new UnsupportedOperationException("loadOrCreate not implemented on this repository");
    }
}
