package com.kaua.event.driven.infrastructure.es.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EventJpaRepository extends JpaRepository<EventEntity, String> {

    List<EventEntity> findAllByAggregateId(String aggregateId);
}
