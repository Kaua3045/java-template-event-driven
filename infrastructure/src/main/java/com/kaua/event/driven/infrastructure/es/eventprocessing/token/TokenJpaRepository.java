package com.kaua.event.driven.infrastructure.es.eventprocessing.token;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TokenJpaRepository extends JpaRepository<TokenEntity, Long> {

    Optional<TokenEntity> findByProcessorName(String processorName);

    void deleteByProcessorName(String processorName);
}
