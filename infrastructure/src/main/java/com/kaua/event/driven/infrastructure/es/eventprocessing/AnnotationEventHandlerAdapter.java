package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AllowReplay;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.ResetHandler;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken;
import com.kaua.event.driven.infrastructure.es.inspector.AnnotationHandlerInspector;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class AnnotationEventHandlerAdapter implements EventHandlerMessage {

    private static final Logger log = LoggerFactory.getLogger(AnnotationEventHandlerAdapter.class);

    private final Object annotatedEventListener;
    private final Class<?> listenerType;
    private final AnnotationHandlerInspector<Object> inspector;

    public AnnotationEventHandlerAdapter(Object annotatedEventListener) {
        this.annotatedEventListener = annotatedEventListener;
        this.listenerType = annotatedEventListener.getClass();
        this.inspector = AnnotationHandlerInspector.inspect(
                annotatedEventListener.getClass()
        );
    }

    @Override
    public <M> boolean canHandle(M message) {
        if (message == null) {
            return true;
        }

        switch (message) {
            case TrackedEventMessage<?> trackedEventMessage -> {
                log.debug("Checking if can handle tracked event message {}", trackedEventMessage);
                return inspector.getHandlers(listenerType)
                        .anyMatch(h -> h.canHandle(trackedEventMessage));
            }
            case DomainEvent domainEvent -> {
                log.debug("Checking if can handle domain event {}", domainEvent);
                return inspector.getHandlers(listenerType)
                        .anyMatch(h -> h.canHandle(domainEvent));
            }
            default -> {
                return false;
            }
        }
    }

    @Override
    public Object handle(DomainEvent message) throws Exception {
        Optional<MessageHandlingMember<? super Object>> handler = inspector.getHandlers(listenerType)
                .filter(h -> h.canHandle(message))
                .findFirst();

        if (handler.isPresent() && message instanceof TrackedEventMessage<?> trackedEventMessage) {
            log.debug("Handling tracked event {} with target {}", trackedEventMessage, annotatedEventListener);

            if (handler.get().hasAnnotation(AllowReplay.class)) {
                final var aAnnotationValue = handler.get().getAnnotation(AllowReplay.class).get();
                if (!aAnnotationValue.value() && ReplayToken.isReplay(trackedEventMessage)) {
                    log.debug("Handling tracked event {} and check is allow replay false, skipping", trackedEventMessage);
                    return null;
                }

                log.debug("Handling tracked event {} and check is allow replay true, continue", trackedEventMessage);
                return handler.get().handle(trackedEventMessage, annotatedEventListener);
            }

            return handler.get().handle(trackedEventMessage, annotatedEventListener);
        } else if (handler.isPresent() && message instanceof DomainEvent domainEvent) {
            log.debug("Handling domain event {} with target {}", domainEvent, annotatedEventListener);
            return handler.get().handle(domainEvent, annotatedEventListener);
        }

        throw new NoHandlerForEventException(message);
    }

    @Override
    public void prepareReset() {
        Optional<MessageHandlingMember<? super Object>> handler = inspector.getHandlers(listenerType)
                .filter(h -> h.hasAnnotation(ResetHandler.class))
                .findFirst();

        if (handler.isPresent()) {
            try {
                handler.get().handle(null, annotatedEventListener);
            } catch (Throwable e) {
                log.error("Error on call reset handler method for [{}]", listenerType.getSimpleName(), e);
                throw e;
            }
        }
    }

    @Override
    public boolean supportsReset() {
        return inspector.getHandlers(listenerType)
                .anyMatch(h -> h.hasAnnotation(ResetHandler.class));
    }
}
