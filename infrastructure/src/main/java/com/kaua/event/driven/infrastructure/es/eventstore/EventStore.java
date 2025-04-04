package com.kaua.event.driven.infrastructure.es.eventstore;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;

import java.time.Instant;
import java.util.List;

public interface EventStore extends EventBus {

    List<DomainEvent> readEvents(String aggregateIdentifier);

    List<DomainEvent> readFirstEvents();
}
