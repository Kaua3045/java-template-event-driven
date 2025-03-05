package com.kaua.event.driven.domain.exceptions;

import com.kaua.event.driven.domain.commands.Command;

public class NoHandlerForCommandException extends NoStackTraceException {

    public NoHandlerForCommandException(Command command) {
        super("No matching handler available to handle command [%s]"
                .formatted(command.getClass().getSimpleName()));
    }
}
