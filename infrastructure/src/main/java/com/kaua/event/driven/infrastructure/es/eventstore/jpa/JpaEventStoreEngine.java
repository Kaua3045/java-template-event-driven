package com.kaua.event.driven.infrastructure.es.eventstore.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class JpaEventStoreEngine implements EventStoreEngine {

    private static final Logger log = LoggerFactory.getLogger(JpaEventStoreEngine.class);

    private final EventJpaRepository eventJpaRepository;

    public JpaEventStoreEngine(final EventJpaRepository eventJpaRepository) {
        this.eventJpaRepository = Objects.requireNonNull(eventJpaRepository);
    }

    @Override
    public void store(final DomainEvent domainEvent) {
        log.debug("Storing event: {}", domainEvent);

        if (domainEvent == null) {
            throw new InternalErrorException("DomainEvent cannot be null");
        }

        eventJpaRepository.save(EventEntity.toEntity(domainEvent));
        log.info("Event stored: {}", domainEvent);
    }

    @Override
    public void store(final List<DomainEvent> domainEvents) {
        log.debug("Storing events: {}", domainEvents.size());

        if (domainEvents.isEmpty()) {
            throw new InternalErrorException("DomainEvents cannot be empty");
        }

        final var aEntities = domainEvents.stream()
                .map(EventEntity::toEntity)
                .toList();
        eventJpaRepository.saveAll(aEntities);
        log.info("Events stored: {}", domainEvents.size());
    }

    @Override
    public List<DomainEvent> readEvents(final String aggregateIdentifier) {
        return this.eventJpaRepository.findAllByAggregateId(aggregateIdentifier)
                .stream().map(EventEntity::toDomain)
                .toList();
    }
}
