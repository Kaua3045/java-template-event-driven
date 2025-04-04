package com.kaua.event.driven.utils;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventStore implements EventStore {

    private final ConcurrentHashMap<String, List<DomainEvent>> events = new ConcurrentHashMap<>();

    @Override
    public List<DomainEvent> readEvents(String aggregateIdentifier) {
        return this.events.getOrDefault(aggregateIdentifier, Collections.emptyList());
    }

    @Override
    public List<DomainEvent> readFirstEvents() {
        throw new UnsupportedOperationException("Not implemented yet");
    }


    @Override
    public void publish(DomainEvent event) {
        this.events.computeIfAbsent(event.aggregateId(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(event);
    }
}
