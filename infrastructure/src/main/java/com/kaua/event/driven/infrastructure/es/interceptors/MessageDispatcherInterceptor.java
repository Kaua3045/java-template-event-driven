package com.kaua.event.driven.infrastructure.es.interceptors;

import jakarta.annotation.Nonnull;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public interface MessageDispatcherInterceptor<T> {

    @Nonnull
    default T handle(@Nonnull T message) {
        return handle(Collections.singletonList(message)).apply(0, message);
    }

    /*
    * This method returns a function that can be invoked to obtain a
    * modified version of messages at each position in the list.
    * */
    @Nonnull
    BiFunction<Integer, T, T> handle(@Nonnull List<? extends T> messages);
}
