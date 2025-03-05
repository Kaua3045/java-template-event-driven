package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConflictAggregateVersionExceptionTest extends UnitTest {

    @Test
    void shouldCreateConflictAggregateVersionException() {
        String aggregateId = "aggregateId";
        long expectedVersion = 1L;
        long actualVersion = 2L;

        ConflictAggregateVersionException exception = new ConflictAggregateVersionException(aggregateId, expectedVersion, actualVersion);

        Assertions.assertEquals("Conflict detected for aggregate " + aggregateId + ". Expected version " + expectedVersion + " but was " + actualVersion, exception.getMessage());
    }
}
