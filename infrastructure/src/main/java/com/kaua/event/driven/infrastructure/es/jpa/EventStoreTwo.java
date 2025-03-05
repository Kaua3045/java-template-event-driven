package com.kaua.event.driven.infrastructure.es.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.eventbus.AbstractEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class EventStoreTwo extends AbstractEventBus implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStoreTwo.class);

    private final EventJpaRepository eventJpaRepository;

    protected EventStoreTwo(Map<Class<?>, AggregateModel<?>> aggregates, EventJpaRepository eventJpaRepository) {
        super(aggregates);
        this.eventJpaRepository = eventJpaRepository;
    }

    @Override
    public void handle(DomainEvent event) {
        saveEvent(event);
        log.debug("Event saved: {}", event);
        super.handle(event);
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

    private void saveEvent(DomainEvent event) {
        eventJpaRepository.save(new EventEntity(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                Json.writeValueAsString(event),
                event.aggregateVersion()
        ));
    }
}
