package com.kaua.event.driven.infrastructure.es.eventstore.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface EventJpaRepository extends JpaRepository<EventEntity, String> {

    List<EventEntity> findAllByAggregateId(String aggregateId);

    List<EventEntity> findTop10ByOccurredOnAfterOrderByOccurredOnDesc(Instant occurredOn);

    List<EventEntity> findTop10ByOrderByOccurredOnAsc();
}
