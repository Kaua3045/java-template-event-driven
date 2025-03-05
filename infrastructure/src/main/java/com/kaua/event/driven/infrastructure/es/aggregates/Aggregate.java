package com.kaua.event.driven.infrastructure.es.aggregates;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Aggregate<T> {

    String type();

    default String identifierAsString() {
        return Objects.toString(identifier(), null);
    }

    Object identifier();

    Long version();

    Object handle(Object message);

    <R> R invoke(Function<T, R> invocation);

    void execute(Consumer<T> invocation);

    boolean isDeleted();

    Class<? extends T> rootType();
}
