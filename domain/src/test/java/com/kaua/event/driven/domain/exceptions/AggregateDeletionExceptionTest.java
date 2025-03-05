package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AggregateDeletionExceptionTest extends UnitTest {

    @Test
    void shouldCreateAggregateDeletionException() {
        final var aggregateIdentifier = "aggregateIdentifier";

        final var exception = new AggregateDeletionException(aggregateIdentifier);

        Assertions.assertEquals("Aggregate with identifier %s not found. It has been deleted"
                        .formatted(aggregateIdentifier),
                exception.getMessage());
    }
}
