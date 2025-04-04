package com.kaua.event.driven.infrastructure.es.eventstore.outbox;

import com.kaua.event.driven.domain.events.DomainEvent;

import java.util.List;

public interface OutboxStoreEngine {

    void store(DomainEvent domainEvent);

    void store(List<DomainEvent> domainEvents);

    List<DomainEvent> findTop50ByStatusOrderByOccurredOnAsc(OutboxStatus status);
}
