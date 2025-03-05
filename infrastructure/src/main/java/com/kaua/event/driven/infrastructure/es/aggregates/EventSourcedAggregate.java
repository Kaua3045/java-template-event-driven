package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;

import java.util.List;

public class EventSourcedAggregate<T> extends AnnotatedAggregate<T> {

//    private boolean initializing = false;

    public EventSourcedAggregate(AggregateModel<T> model, EventBus eventBus, T aggregateRoot) {
        super(model, eventBus, aggregateRoot);
    }

//    public void initializeState(List<DomainEvent> domainEvents) {
//        execute(r -> {
//            this.initializing = true;
//            try {
//                domainEvents.forEach(this::publish);
////                initSequence(domainEvents.getLastSequence());
//            } finally {
//                this.initializing = false;
////                snapshotTrigger.initializationFinished();
//            }
//        });
//    }
}
