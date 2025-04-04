package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;

public interface EventHandlerMessage extends MessageHandler<DomainEvent> {

    Object handle(DomainEvent event) throws Exception;

    default void prepareReset() {}

    default boolean supportsReset() {
        return true;
    }
}
