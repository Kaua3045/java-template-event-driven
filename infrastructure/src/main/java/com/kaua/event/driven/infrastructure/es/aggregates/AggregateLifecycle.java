package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.scope.Scope;

public abstract class AggregateLifecycle extends Scope {

    public static ApplyMore apply(Object payload) {
        return AggregateLifecycle.getInstance().doApply(payload);
    }

    public static boolean isLive() {
        return AggregateLifecycle.getInstance().getIsLive();
    }

    public static Long getVersion() {
        return AggregateLifecycle.getInstance().version();
    }

    public static void markDeleted() {
        AggregateLifecycle.getInstance().doMarkDeleted();
    }

    protected static AggregateLifecycle getInstance() {
        return Scope.getCurrentScope();
    }

    protected abstract <T> ApplyMore doApply(T payload);

    // TODO ?
    protected abstract boolean getIsLive();

    protected abstract Long version();

    protected abstract void doMarkDeleted();

    protected void execute(Runnable task) {
        try {
            executeWithResult(() -> {
                task.run();
                return null;
            });
        } catch (Exception ex) {
            throw new InternalErrorException("Unexpected Error occurred on execute task in aggregate lifecycle", ex);
        }
    }

    protected abstract String type();

    protected abstract Object identifier();
}
