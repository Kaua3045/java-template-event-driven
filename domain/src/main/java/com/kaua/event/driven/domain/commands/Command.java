package com.kaua.event.driven.domain.commands;

import java.time.Instant;

public interface Command {

    String commandId(); // command identifier (UUID)

    String commandType(); // command type (OrderCreated, OrderUpdated, OrderDeleted, etc.)

    Instant occurredOn(); // command occurred date (2021-07-01T00:00:00Z)

    String aggregateId(); // aggregate identifier (UUID)

    long aggregateVersion(); // event/aggregate version (1, 2, 3, etc.)

    String traceId(); // trace identifier (UUID) podemos colocar se quiser
}

