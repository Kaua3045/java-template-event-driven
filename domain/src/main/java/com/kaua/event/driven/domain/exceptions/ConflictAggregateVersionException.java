package com.kaua.event.driven.domain.exceptions;

public class ConflictAggregateVersionException extends NoStackTraceException {

    private String aggregateId;
    private long expectedVersion;
    private long actualVersion;

    public ConflictAggregateVersionException(String aggregateId, long expectedVersion, long actualVersion) {
        super("Conflict detected for aggregate " + aggregateId + ". Expected version " + expectedVersion + " but was " + actualVersion);
        this.aggregateId = aggregateId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public long getExpectedVersion() {
        return expectedVersion;
    }

    public long getActualVersion() {
        return actualVersion;
    }
}
