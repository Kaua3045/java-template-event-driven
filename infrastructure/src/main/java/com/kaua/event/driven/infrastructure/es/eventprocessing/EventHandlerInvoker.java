package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;

public interface EventHandlerInvoker {

    boolean canHandle(DomainEvent event);

    void handle(DomainEvent event) throws Exception;

    default boolean supportsReset() {
        return false;
    }

    default void performReset() {}
}
