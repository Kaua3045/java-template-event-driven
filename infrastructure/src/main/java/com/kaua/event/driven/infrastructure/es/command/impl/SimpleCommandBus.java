package com.kaua.event.driven.infrastructure.es.command.impl;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.exceptions.NoHandlerForCommandException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.CommandHandlerImpl;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.es.command.callback.LoggingCallback;
import com.kaua.event.driven.infrastructure.es.interceptors.DefaultInterceptorChain;
import com.kaua.event.driven.infrastructure.es.interceptors.InterceptorChain;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageDispatcherInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.uow.*;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

// TODO implement dispatcher interceptors
public class SimpleCommandBus implements CommandBus {

    private static final Logger log = LoggerFactory.getLogger(SimpleCommandBus.class);

    private final Map<Class<?>, AggregateModel<?>> aggregates;
    private final Map<Class<?>, CommandHandlerImpl<?>> commandHandlers;
    private final List<MessageHandlerInterceptor<? super Command>> interceptors;
    private final List<MessageDispatcherInterceptor<? super Command>> dispatcherInterceptors;
    private final CommandCallBack<Object, Object> defaultCommandCallback;
    private final TransactionManager transactionManager;
    private RollbackConfiguration rollbackConfiguration;

    public static Builder builder() {
        return new Builder();
    }

    public SimpleCommandBus(
            Builder builder
    ) {
        this.aggregates = builder.aggregates;
        this.commandHandlers = builder.commandHandlers;
        this.interceptors = new ArrayList<>();
        this.dispatcherInterceptors = new ArrayList<>();
        this.defaultCommandCallback = builder.defaultCommandCallback;
        this.transactionManager = builder.transactionManager;
        this.rollbackConfiguration = builder.rollbackConfiguration;
    }

    @Override
    public void dispatch(Command command) {
        dispatch(command, defaultCommandCallback);
    }

    @Override
    public <C, R> void dispatch(@Nonnull Command command, @Nonnull CommandCallBack<? super C, ? super R> callback) {
        log.debug("Dispatching command with callback");
        final var aAggregate = getAggregateClassForCommand(command);

        if (aAggregate == null) {
            throw new NoHandlerForCommandException(command);
        }

        final var model = aggregates.get(aAggregate);

        if (model == null) {
            throw NotFoundException.withMessage("Aggregate model not found for class %s and command %s"
                    .formatted(aAggregate.getSimpleName(), command.getClass().getSimpleName()));
        }

        final var aHandler = commandHandlers.get(aAggregate);
        try {
            handle(command, aHandler, callback, aAggregate.getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerHandlerInterceptor(MessageHandlerInterceptor<? super Command> interceptor) {
        interceptors.add(interceptor);
    }

    @Override
    public void registerDispatcherInterceptor(MessageDispatcherInterceptor<? super Command> interceptor) {
        dispatcherInterceptors.add(interceptor);
    }

    public <C, R> void handle(
            Command command,
            CommandHandlerImpl<?> aHandler,
            CommandCallBack<C, R> callback,
            Object aggregate
    ) {
        final var uow = DefaultUnitOfWork.startAndGet(command);
        uow.attachTransaction(transactionManager);

        InterceptorChain chain = new DefaultInterceptorChain<>(
                m -> aHandler.handle(command, createAggregateFactory(aggregate)),
                interceptors,
                uow
        );

        final var aResult = uow.executeWithResult(chain::process,
                rollbackConfiguration);
        callback.onResult((C) command, (ResultMessage<R>) aResult);
    }

    private Class<?> getAggregateClassForCommand(Object command) {
        return aggregates.entrySet().stream()
                .filter(entry ->
                        entry.getValue().commandHandlers().containsKey(command.getClass()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    private <T> Callable<T> createAggregateFactory(Object aggregate) {
        return () -> (T) aggregate;
    }

    public void setRollbackConfiguration(@Nonnull RollbackConfiguration rollbackConfiguration) {
        this.rollbackConfiguration = rollbackConfiguration;
    }

    public static class Builder {
        private Map<Class<?>, AggregateModel<?>> aggregates;
        private Map<Class<?>, CommandHandlerImpl<?>> commandHandlers;
        private TransactionManager transactionManager = NoTransactionManager.INSTANCE;
        private RollbackConfiguration rollbackConfiguration = RollbackConfigurationType.UNCHECKED_EXCEPTIONS;
        private CommandCallBack<Object, Object> defaultCommandCallback = LoggingCallback.INSTANCE;

        public Builder aggregates(@Nonnull Map<Class<?>, AggregateModel<?>> aggregates) {
            this.aggregates = aggregates;
            return this;
        }

        public Builder commandHandlers(@Nonnull Map<Class<?>, CommandHandlerImpl<?>> commandHandlers) {
            this.commandHandlers = commandHandlers;
            return this;
        }

        public Builder transactionManager(@Nonnull TransactionManager transactionManager) {
            this.transactionManager = transactionManager;
            return this;
        }

        public Builder rollbackConfiguration(@Nonnull RollbackConfiguration rollbackConfiguration) {
            this.rollbackConfiguration = rollbackConfiguration;
            return this;
        }

        public Builder defaultCommandCallback(CommandCallBack<Object, Object> defaultCommandCallback) {
            this.defaultCommandCallback = defaultCommandCallback == null ? LoggingCallback.INSTANCE : defaultCommandCallback;
            return this;
        }

        public SimpleCommandBus build() {
            return new SimpleCommandBus(this);
        }
    }
}
