package com.kaua.event.driven.infrastructure.es.eventprocessing.invoker;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerMessage;

import java.util.List;

public class SimpleEventHandlerInvoker implements EventHandlerInvoker {

    private final List<EventHandlerMessage> eventHandlingComponents;

    public SimpleEventHandlerInvoker(List<EventHandlerMessage> eventHandlingComponents) {
        this.eventHandlingComponents = eventHandlingComponents;
    }

    @Override
    public boolean canHandle(DomainEvent event) {
        return eventHandlingComponents
                .stream()
                .anyMatch(handler -> handler.canHandle(event));
    }

    @Override
    public void handle(DomainEvent event) throws Exception {
        for (var handler : eventHandlingComponents) {
            if (handler.canHandle(event)) {
                handler.handle(event);
            }
        }
    }

    @Override
    public boolean supportsReset() {
        return eventHandlingComponents
                .stream()
                .anyMatch(EventHandlerMessage::supportsReset);
    }

    @Override
    public void performReset() {
        for (EventHandlerMessage eventHandlingComponent : eventHandlingComponents) {
            eventHandlingComponent.prepareReset();
        }
    }
}
