package com.kaua.event.driven.infrastructure.es.aggregates;

import java.util.function.Supplier;

public interface ApplyMore {

    ApplyMore andThenApply(Supplier<?> payloadOrMessageSupplier);

    default ApplyMore andThenApplyIf(Supplier<Boolean> condition, Supplier<?> payloadOrMessageSupplier) {
        if (condition.get()) {
            return andThenApply(payloadOrMessageSupplier);
        } else {
            return this;
        }
    }

    ApplyMore andThen(Runnable runnable);

    ApplyMore andThenIf(Supplier<Boolean> condition, Runnable runnable);
}
