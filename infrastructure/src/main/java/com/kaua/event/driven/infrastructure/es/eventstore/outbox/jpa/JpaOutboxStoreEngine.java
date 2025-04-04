package com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStatus;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class JpaOutboxStoreEngine implements OutboxStoreEngine {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxStoreEngine.class);

    private final OutboxJpaRepository outboxJpaRepository;

    public JpaOutboxStoreEngine(final OutboxJpaRepository outboxJpaRepository) {
        this.outboxJpaRepository = Objects.requireNonNull(outboxJpaRepository);
    }

    @Override
    public void store(final DomainEvent domainEvent) {
        log.debug("Storing event: {}", domainEvent);
        this.outboxJpaRepository.save(OutboxEntity.toEntity(domainEvent));
    }

    @Override
    public void store(final List<DomainEvent> domainEvents) {
        log.debug("Storing events: {}", domainEvents.size());
        final var aEntities = domainEvents.stream()
                .map(OutboxEntity::toEntity)
                .toList();
        this.outboxJpaRepository.saveAll(aEntities);
    }

    @Override
    public List<DomainEvent> findTop50ByStatusOrderByOccurredOnAsc(final OutboxStatus status) {
        log.debug("Finding top 50 events by status: {} and order by occurred on [ASC]", status);
        return this.outboxJpaRepository.findTop50ByStatusOrderByOccurredOnAsc(status)
                .stream().map(OutboxEntity::toDomain)
                .toList();
    }
}
