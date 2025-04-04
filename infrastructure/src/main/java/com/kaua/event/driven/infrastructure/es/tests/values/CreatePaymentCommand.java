package com.kaua.event.driven.infrastructure.es.tests.values;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.domain.utils.InstantUtils;

import java.time.Instant;

public record CreatePaymentCommand(
        String commandId,
        String commandType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String traceId,
        String text
) implements Command {

    public CreatePaymentCommand(
            String text
    ) {
        this(
                IdentifierUtils.generateNewId(),
                CreatePaymentCommand.class.getSimpleName(),
                InstantUtils.now(),
                IdentifierUtils.generateNewId(),
                0L,
                IdentifierUtils.generateNewId(),
                text
        );
    }
}
