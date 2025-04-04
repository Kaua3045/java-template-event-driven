package com.kaua.event.driven.infrastructure.es.eventstore;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.validation.AssertionConcern;
import com.kaua.event.driven.infrastructure.configurations.properties.eventstore.EventStoreProperties;
import com.kaua.event.driven.infrastructure.es.eventbus.AbstractEventBus;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventStoreImpl extends AbstractEventBus implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStoreImpl.class);

    private final EventStoreEngine eventStoreEngine;
    private final OutboxStoreEngine outboxStoreEngine;
    private final EventStoreProperties eventStoreProperties;

    public static Builder builder() {
        return new Builder();
    }

    public EventStoreImpl(Builder builder) {
        this.eventStoreEngine = builder.eventStoreEngine;
        this.outboxStoreEngine = builder.outboxStoreEngine;
        this.eventStoreProperties = builder.eventStoreProperties;
    }

    @Override
    protected void prepareCommit(List<? extends DomainEvent> events) {
        log.debug("Preparing to save events: {}", events);
        final var aDomainEvents = events.stream()
                .map(it -> (DomainEvent) it)
                .toList();

        if (eventStoreProperties.isOutboxEnabled()) {
            log.debug("Outbox is enabled, saving events to outbox");
            eventStoreEngine.store(aDomainEvents);
            outboxStoreEngine.store(aDomainEvents);
        }

        eventStoreEngine.store(aDomainEvents);

        super.prepareCommit(events);
    }

    @Override
    public List<DomainEvent> readEvents(String aggregateIdentifier) {
        return eventStoreEngine.readEvents(aggregateIdentifier);
    }

    @Override
    public List<DomainEvent> readFirstEvents() {
        // TODO implement this method
        throw new UnsupportedOperationException("Not implemented yet, need to implement a way to read the events");
    }

    public static class Builder implements AssertionConcern {

        private EventStoreEngine eventStoreEngine;
        private OutboxStoreEngine outboxStoreEngine;
        private EventStoreProperties eventStoreProperties;

        public Builder eventStoreEngine(EventStoreEngine eventStoreEngine) {
            this.eventStoreEngine = assertArgumentNotNull(
                    eventStoreEngine,
                    "EventStoreEngine",
                    "should not be null, because it is mandatory"
            );
            return this;
        }

        public Builder eventStoreProperties(EventStoreProperties eventStoreProperties) {
            this.eventStoreProperties = assertArgumentNotNull(
                    eventStoreProperties,
                    "EventStoreProperties",
                    "should not be null, because it is mandatory"
            );
            return this;
        }

        public Builder outboxStoreEngine(OutboxStoreEngine outboxStoreEngine) {
            this.outboxStoreEngine = outboxStoreEngine;
            return this;
        }

        public EventStoreImpl build() {
            return new EventStoreImpl(this);
        }
    }
}
