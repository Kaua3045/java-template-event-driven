package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.commands.Command;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class NoHandlerForCommandExceptionTest extends UnitTest {

    @Test
    void shouldCreateNoHandlerForCommandException() {
        var command = new CommandStub();
        var exception = new NoHandlerForCommandException(command);
        Assertions.assertEquals("No matching handler available to handle command [%s]"
                        .formatted(command.getClass().getSimpleName()),
                exception.getMessage()
        );
    }

    private record CommandStub(
            String commandId,
            String commandType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String traceId
    ) implements Command {

        public CommandStub() {
            this(
                    "commandId",
                    "commandType",
                    Instant.now(),
                    "aggregateId",
                    1L,
                    "traceId"
            );
        }
    }
}
