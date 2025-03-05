package com.kaua.event.driven.infrastructure.es.command;

import com.kaua.event.driven.domain.commands.Command;

public interface CommandGateway {

    void send(Command command);

    <R> R sendAndWait(Command command);
}
