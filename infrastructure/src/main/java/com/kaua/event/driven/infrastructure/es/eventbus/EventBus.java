package com.kaua.event.driven.infrastructure.es.eventbus;

import com.kaua.event.driven.domain.events.DomainEvent;

public interface EventBus {

    void publish(DomainEvent event);
}
