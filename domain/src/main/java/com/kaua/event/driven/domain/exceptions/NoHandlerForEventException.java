package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.events.DomainEvent;

public class NoHandlerForEventException extends NoStackTraceException {

    public NoHandlerForEventException(DomainEvent event) {
        super("No matching handler available to handle event [%s]"
                .formatted(event.getClass().getSimpleName()));
    }
}
