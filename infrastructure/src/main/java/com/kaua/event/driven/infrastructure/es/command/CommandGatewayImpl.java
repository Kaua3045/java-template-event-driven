package com.kaua.event.driven.infrastructure.es.command;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.command.callback.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CommandGatewayImpl implements CommandGateway {

    private static final Logger log = LoggerFactory.getLogger(CommandGatewayImpl.class);

    private final CommandBus commandBus;

    public CommandGatewayImpl(CommandBus commandBus) {
        this.commandBus = commandBus;
    }

    @Override
    public void send(Command command) {
        log.info("Sending command: {}", command);
        commandBus.dispatch(command);
    }

    @Override
    public <R> R sendAndWait(Command command) {
        log.info("Sending command and waiting for response: {}", command);
        final FutureCallback<Object, R> futureCallback = new FutureCallback<>();
        commandBus.dispatch(command, futureCallback);

        final var aResult = futureCallback.getResult();

        if (aResult.isExceptional()) {
            throw new InternalErrorException(aResult.getExceptionResult());
        }

        log.info("Command {} executed with result {}", command, aResult.getResult());
        return aResult.getResult();
    }
}
