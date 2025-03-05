package com.kaua.event.driven.infrastructure.es.conflict;

import com.kaua.event.driven.domain.events.DomainEvent;

import java.util.List;
import java.util.function.Predicate;

public interface ConflictResolver {

    void ensureConflictsResolved();

    <T extends Exception> void detectConflicts(Predicate<List<DomainEvent>> predicate, ConflictExceptionSupplier<T> exceptionSupplier) throws T;
}
