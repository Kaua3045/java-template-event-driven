package com.kaua.event.driven.infrastructure.es.conflict;

import com.kaua.event.driven.domain.exceptions.NoStackTraceException;

@FunctionalInterface
public interface ConflictExceptionSupplier<T extends Exception> {

    T supplyException(NoStackTraceException exception);
}
