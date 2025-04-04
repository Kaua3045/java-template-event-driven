package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.exceptions.NoHandlerForCommandException;
import com.kaua.event.driven.domain.validation.AssertionConcern;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class AggregateCommandHandlerAnnotated<T> implements MessageHandler<Command> {

    private static final Logger log = LoggerFactory.getLogger(AggregateCommandHandlerAnnotated.class);

    private final AggregateRepository<T> repository;
    private final List<MessageHandler<Command>> handlers;

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private AggregateCommandHandlerAnnotated(Builder<T> builder) {
        builder.validate();
        this.repository = builder.repository;

        AggregateModel<T> aggregateModel = builder.buildAggregateModel();
        this.handlers = initializeHandlers(aggregateModel);
    }

    private List<MessageHandler<Command>> initializeHandlers(AggregateModel<T> aggregateModel) {
        List<MessageHandler<Command>> aHandlersFound = new ArrayList<>();

        aggregateModel.commandHandlers()
                .values()
                .forEach((commandHandler) -> initializeHandler(
                        aggregateModel, commandHandler, aHandlersFound
                ));

        return aHandlersFound;
    }

    private void initializeHandler(AggregateModel<T> aggregateModel, MessageHandlingMember<? super T> commandHandler, List<MessageHandler<Command>> aHandlersFound) {
        MessageHandler<Command> messageHandler = new AggregateCommandHandler(commandHandler, () -> aggregateModel.entityClass().getDeclaredConstructor().newInstance());
        aHandlersFound.add(messageHandler);
        log.debug("Command handler {} initialized for aggregate {}", commandHandler, aggregateModel.entityClass().getSimpleName());
    }

    @Override
    public <M> boolean canHandle(M message) {
        log.debug("Checking if can handle command {}", message);
        return handlers
                .stream()
                .anyMatch(ch -> ch.canHandle(message));
    }

    @Override
    public Object handle(Command message) throws Exception {
        log.debug("Handling command {}", message);
        return handlers
                .stream()
                .filter(ch -> ch.canHandle(message))
                .findFirst()
                .orElseThrow(() -> new NoHandlerForCommandException(message))
                .handle(message);
    }

    public static class Builder<T> implements AssertionConcern {

        private AggregateRepository<T> repository;
        private Class<T> aggregateType;
        private AggregateModel<T> aggregateModel;

        public Builder<T> repository(AggregateRepository<T> repository) {
            assertArgumentNotNull(repository, "repository", "should not be null");
            this.repository = repository;
            return this;
        }

        public Builder<T> aggregateType(Class<T> aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        public Builder<T> aggregateModel(AggregateModel<?> aggregateModel) {
            assertArgumentNotNull(aggregateModel, "aggregateModel", "should not be null");
            this.aggregateModel = (AggregateModel<T>) aggregateModel;
            return this;
        }

        private AggregateModel<T> buildAggregateModel() {
            if (aggregateModel == null) {
                return new DefaultAggregateModel<>(aggregateType);
            } else {
                return aggregateModel;
            }
        }

        public AggregateCommandHandlerAnnotated<T> build() {
            return new AggregateCommandHandlerAnnotated<>(this);
        }

        protected void validate() {
            assertArgumentNotNull(repository, "repository", "should not be null");
            if (aggregateModel == null) {
                assertArgumentNotNull(aggregateType, "aggregateType", "should not be null");
                return;
            }

            assertArgumentNotNull(aggregateModel, "aggregateModel", "should not be null");
        }
    }

    private class AggregateCommandHandler implements MessageHandler<Command> {

        private final MessageHandlingMember<? super T> handler;
        private final Callable<T> factoryMethod;

        public AggregateCommandHandler(MessageHandlingMember<? super T> handler, Callable<T> factoryMethod) {
            this.handler = handler;
            this.factoryMethod = factoryMethod;
        }

        @Override
        public <M> boolean canHandle(M message) {
            return handler.canHandle(message);
        }

        @Override
        public Object handle(Command message) throws Exception {
            return repository.loadOrCreate(
                    message.aggregateId(),
                    factoryMethod
            ).handle(message);
        }
    }
}
