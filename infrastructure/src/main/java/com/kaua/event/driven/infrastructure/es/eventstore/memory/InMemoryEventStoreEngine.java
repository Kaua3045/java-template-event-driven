package com.kaua.event.driven.infrastructure.es.eventstore.memory;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventStoreEngine implements EventStoreEngine {

    private static final Logger log = LoggerFactory.getLogger(InMemoryEventStoreEngine.class);

    private final ConcurrentHashMap<String, List<DomainEvent>> events = new ConcurrentHashMap<>();

    @Override
    public void store(final DomainEvent domainEvent) {
        log.debug("Storing event: {}", domainEvent);

        if (domainEvent == null) {
            throw new InternalErrorException("DomainEvent cannot be null");
        }

        final var aggregateId = domainEvent.aggregateId();
        final var aEvents = new ArrayList<DomainEvent>();
        aEvents.add(domainEvent);

        if (events.containsKey(aggregateId)) {
            aEvents.addAll(events.get(aggregateId));
            events.put(aggregateId, aEvents);
        } else {
            events.put(aggregateId, aEvents);
        }
    }

    @Override
    public void store(final List<DomainEvent> domainEvents) {
        log.debug("Storing events: {}", domainEvents.size());

        if (domainEvents.isEmpty()) {
            throw new InternalErrorException("DomainEvents cannot be empty");
        }

        if (events.containsKey(domainEvents.getFirst().aggregateId())) {
            final var aEvents = new ArrayList<DomainEvent>();
            aEvents.addAll(domainEvents);
            aEvents.addAll(events.get(domainEvents.getFirst().aggregateId()));
            events.put(domainEvents.getFirst().aggregateId(), aEvents);
        } else {
            events.put(domainEvents.getFirst().aggregateId(), domainEvents);
        }
    }

    @Override
    public List<DomainEvent> readEvents(String aggregateIdentifier) {
        return events.getOrDefault(aggregateIdentifier, new ArrayList<>());
    }
}
