package com.kaua.event.driven.infrastructure.es.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import org.springframework.stereotype.Component;

import java.util.List;

//@Component
public class EventStoreImpl implements EventStore {

    private final EventJpaRepository eventJpaRepository;

    public EventStoreImpl(EventJpaRepository eventJpaRepository) {
        this.eventJpaRepository = eventJpaRepository;
    }

    @Override
    public List<DomainEvent> readEvents(String aggregateIdentifier) {
        return eventJpaRepository.findAllByAggregateId(aggregateIdentifier)
                .stream().map(it -> {
                    try {
                        return (DomainEvent) Json.readValue(it.getData(), Class.forName(it.getType()));
                    } catch (ClassNotFoundException e) {
                        throw new InternalErrorException("Error on deserialize event, class not found", e);
                    }
                }).toList();
    }

    @Override
    public void publish(DomainEvent event) {

    }
}
