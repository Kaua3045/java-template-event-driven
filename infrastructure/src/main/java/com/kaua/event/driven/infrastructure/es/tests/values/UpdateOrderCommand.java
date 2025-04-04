package com.kaua.event.driven.infrastructure.es.tests.values;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.domain.utils.InstantUtils;

import java.time.Instant;

public record UpdateOrderCommand(
        String commandId,
        String commandType,
        Instant occurredOn,
        String aggregateId,
        long aggregateVersion,
        String traceId,
        String description
) implements Command {

    public UpdateOrderCommand(
            String aggregateId,
            String description
    ) {
        this(
                IdentifierUtils.generateNewId(),
                UpdateOrderCommand.class.getSimpleName(),
                InstantUtils.now(),
                aggregateId,
                0L,
                IdentifierUtils.generateNewId(),
                description
        );
    }
}
