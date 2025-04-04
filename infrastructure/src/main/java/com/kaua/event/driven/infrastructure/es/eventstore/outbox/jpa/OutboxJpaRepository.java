package com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa;

import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStatus;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxJpaRepository extends JpaRepository<OutboxEntity, String> {

    @QueryHints({
            @QueryHint(
                    name = AvailableSettings.JAKARTA_LOCK_TIMEOUT,
                    value = "-2" // SKIP LOCKED
            )
    })
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<OutboxEntity> findTop50ByStatusOrderByOccurredOnAsc(final OutboxStatus status);
}
