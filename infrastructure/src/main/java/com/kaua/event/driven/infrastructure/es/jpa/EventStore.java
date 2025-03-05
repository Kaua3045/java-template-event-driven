package com.kaua.event.driven.infrastructure.es.jpa;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;

import java.util.List;

public interface EventStore extends EventBus {

    List<DomainEvent> readEvents(String aggregateIdentifier);
}
