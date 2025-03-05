package com.kaua.event.driven.infrastructure.es.command;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.EventSourcingRepository;

import java.util.concurrent.Callable;

public class CommandHandlerImpl<T> {

    private final EventSourcingRepository<T> repository;

    public CommandHandlerImpl(EventSourcingRepository<T> repository) {
        this.repository = repository;
    }

    public Object handle(Object command, Callable<T> aggregateType) {
        try {
            // TODO passar o version também
            Aggregate<T> aggregate = repository.loadOrCreate(
                    getAggregateId(command),
                    aggregateType
            );

            return aggregate.handle(command);
        } catch (Exception e) {
            throw new InternalErrorException("Failed on handle command", e);
        }
    }

    private String getAggregateId(Object command) {
        if (command instanceof Command) {
            return ((Command) command).aggregateId();
        } else {
            throw new InternalErrorException("Command must be an instance of Command");
        }
    }
}
