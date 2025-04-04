package com.kaua.event.driven;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.ApplyMore;
import com.kaua.event.driven.infrastructure.es.scope.ScopeDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

public class StubAggregateLifecycle extends AggregateLifecycle {

    private static final String AGGREGATE_TYPE = "stubAggregate";

    private Runnable registration;
    private final List<DomainEvent> appliedMessages = new CopyOnWriteArrayList<>();
    private boolean deleted;

    public void activate() {
        super.startScope();
        this.registration = super::endScope;
    }

    public void close() {
        if (registration != null) {
            registration.run();
        }
        registration = null;
    }

    @Override
    protected boolean getIsLive() {
        return true;
    }

//    @Override
//    protected <T> Aggregate<T> doCreateNew(Class<T> aggregateType, Callable<T> factoryMethod) throws Exception {
//        return null;
//    }

    @Override
    protected String type() {
        return AGGREGATE_TYPE;
    }

    @Override
    protected Object identifier() {
        return IdentifierUtils.generateNewId();
    }

    @Override
    protected Long version() {
        return 0L;
    }

    @Override
    protected void doMarkDeleted() {
        this.deleted = true;
    }

    @Override
    protected <T> ApplyMore doApply(T payload) {
        appliedMessages.add((DomainEvent) payload);

        return new ApplyMore() {
            @Override
            public ApplyMore andThenApply(Supplier<?> payloadOrMessageSupplier) {
                appliedMessages.add((DomainEvent) payloadOrMessageSupplier.get());
                return this;
            }

            @Override
            public ApplyMore andThen(Runnable runnable) {
                runnable.run();
                return this;
            }

            @Override
            public ApplyMore andThenIf(Supplier<Boolean> condition, Runnable runnable) {
                return null;
            }
        };
    }

    public List<DomainEvent> getAppliedEvents() {
        return appliedMessages;
    }

    public List<Object> getAppliedEventPayloads() {
        return new ArrayList<>(appliedMessages);
    }

    public boolean isMarkedDeleted() {
        return deleted;
    }

    @Override
    public ScopeDescriptor describeScope() {
        return null;
    }
}
