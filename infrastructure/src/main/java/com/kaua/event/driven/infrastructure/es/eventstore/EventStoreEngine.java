package com.kaua.event.driven.infrastructure.es.eventstore;

import com.kaua.event.driven.domain.events.DomainEvent;

import java.util.List;

public interface EventStoreEngine {

    void store(DomainEvent domainEvent);

    void store(List<DomainEvent> domainEvents);

    List<DomainEvent> readEvents(String aggregateIdentifier);
}
