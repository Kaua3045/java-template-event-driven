package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InternalErrorExceptionTest extends UnitTest {

    @Test
    void testCreateInternalErrorException() {
        final var aMessage = "Failed on handle command";
        final var aCause = new Exception();
        InternalErrorException internalErrorException = new InternalErrorException(aMessage, aCause);
        Assertions.assertNotNull(internalErrorException);
        Assertions.assertEquals(aMessage, internalErrorException.getMessage());
        Assertions.assertEquals(aCause, internalErrorException.getCause());
    }

    @Test
    void testCreateInternalErrorExceptionWithMessage() {
        final var aMessage = "Failed on handle command";
        InternalErrorException internalErrorException = new InternalErrorException(aMessage);
        Assertions.assertNotNull(internalErrorException);
        Assertions.assertEquals(aMessage, internalErrorException.getMessage());
    }

    @Test
    void testCreateInternalErrorExceptionWithCause() {
        final var aCause = new Exception();
        InternalErrorException internalErrorException = new InternalErrorException(aCause);
        Assertions.assertNotNull(internalErrorException);
        Assertions.assertEquals(aCause, internalErrorException.getCause());
    }
}
