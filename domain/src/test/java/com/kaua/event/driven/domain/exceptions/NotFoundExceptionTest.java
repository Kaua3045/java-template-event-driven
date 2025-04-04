package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NotFoundExceptionTest extends UnitTest {

    @Test
    void givenAValidIdAndVersion_whenCallWith_thenReturnNotFoundException() {
        final var aId = IdentifierUtils.generateNewId();
        final var aVersion = 0L;
        final var expectedErrorMessage = "Aggregate with identifier %s and version 0 was not found"
                .formatted(aId);

        final var notFoundException = Assertions.assertThrows(
                NotFoundException.class,
                () -> {
                    throw NotFoundException.withIdentifierAndVersion(aId, aVersion);
                }
        );

        Assertions.assertEquals(aId, notFoundException.getIdentifier());
        Assertions.assertEquals(aVersion, notFoundException.getVersion());
        Assertions.assertEquals(expectedErrorMessage, notFoundException.getMessage());
    }

    @Test
    void givenAValidMessage_whenCallWith_thenReturnNotFoundException() {
        final var aMessage = "A message";
        final var expectedErrorMessage = "A message";

        final var notFoundException = Assertions.assertThrows(
                NotFoundException.class,
                () -> {
                    throw NotFoundException.withMessage(aMessage);
                }
        );

        Assertions.assertEquals(expectedErrorMessage, notFoundException.getMessage());
    }

    @Test
    void givenAValidId_whenCallWith_thenReturnNotFoundException() {
        final var aId = IdentifierUtils.generateNewId();
        final var expectedErrorMessage = "Aggregate with identifier %s was not found"
                .formatted(aId);

        final var notFoundException = Assertions.assertThrows(
                NotFoundException.class,
                () -> {
                    throw NotFoundException.withIdentifier(aId);
                }
        );

        Assertions.assertEquals(aId, notFoundException.getIdentifier());
        Assertions.assertEquals(expectedErrorMessage, notFoundException.getMessage());
    }
}
