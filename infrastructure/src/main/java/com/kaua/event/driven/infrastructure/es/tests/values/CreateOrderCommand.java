package com.kaua.event.driven.infrastructure.es.tests.values;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.domain.utils.InstantUtils;

import java.time.Instant;

public record CreateOrderCommand(
        String commandId,
        String commandType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String traceId,
        String description
) implements Command {

    public CreateOrderCommand(
            String description
    ) {
        this(
                IdentifierUtils.generateNewId(),
                CreateOrderCommand.class.getSimpleName(),
                InstantUtils.now(),
                IdentifierUtils.generateNewId(),
                0L,
                IdentifierUtils.generateNewId(),
                description
        );
    }
}
