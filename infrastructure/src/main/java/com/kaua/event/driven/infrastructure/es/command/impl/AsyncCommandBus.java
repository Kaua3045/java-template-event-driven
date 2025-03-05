package com.kaua.event.driven.infrastructure.es.command.impl;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.command.CommandHandlerImpl;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.uow.RollbackConfiguration;
import com.kaua.event.driven.infrastructure.uow.TransactionManager;
import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncCommandBus extends SimpleCommandBus {

    private final ExecutorService executorService;

    public static Builder builder() {
        return new Builder();
    }

    public AsyncCommandBus(Builder builder) {
        super(builder);
        this.executorService = builder.executor;
    }

    @Override
    public <C, R> void handle(Command command, CommandHandlerImpl<?> aHandler, CommandCallBack<C, R> callback, Object aggregate) {
        executorService.execute(() -> super.handle(command, aHandler, callback, aggregate));
    }

    public static class Builder extends SimpleCommandBus.Builder {

        private ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("async-command-bus-", Thread.currentThread().threadId()).factory()
        );

        @Override
        public Builder transactionManager(@Nonnull TransactionManager transactionManager) {
            super.transactionManager(transactionManager);
            return this;
        }

        @Override
        public Builder aggregates(@Nonnull Map<Class<?>, AggregateModel<?>> aggregates) {
            super.aggregates(aggregates);
            return this;
        }

        @Override
        public Builder commandHandlers(@Nonnull Map<Class<?>, CommandHandlerImpl<?>> commandHandlers) {
            super.commandHandlers(commandHandlers);
            return this;
        }

        @Override
        public Builder defaultCommandCallback(@Nonnull CommandCallBack<Object, Object> defaultCommandCallback) {
            super.defaultCommandCallback(defaultCommandCallback);
            return this;
        }

        @Override
        public Builder rollbackConfiguration(@Nonnull RollbackConfiguration rollbackConfiguration) {
            super.rollbackConfiguration(rollbackConfiguration);
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public AsyncCommandBus build() {
            return new AsyncCommandBus(this);
        }
    }
}
