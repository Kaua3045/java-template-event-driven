package com.kaua.event.driven.domain.exceptions;

public class ConflictAggregateVersionException extends NoStackTraceException {

    public ConflictAggregateVersionException(String aggregateId, long expectedVersion, long actualVersion) {
        super("Conflict detected for aggregate " + aggregateId + ". Expected version " + expectedVersion + " but was " + actualVersion);
    }
}
