package com.kaua.event.driven.infrastructure.es.command.impl;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.exceptions.NoHandlerForCommandException;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.es.command.callback.LoggingCallback;
import com.kaua.event.driven.infrastructure.es.interceptors.DefaultInterceptorChain;
import com.kaua.event.driven.infrastructure.es.interceptors.InterceptorChain;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageDispatcherInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import com.kaua.event.driven.infrastructure.uow.*;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SimpleCommandBus implements CommandBus {

    private static final Logger log = LoggerFactory.getLogger(SimpleCommandBus.class);

    // TODO da forma que os command handlers estão hoje
    //  não é possível fazer a busca por nome do command
    // se eu tiver 10 aggregados, vai fazer um for ate achar
    // o annotated handler correto
    // depois vai fazer um for por todos os handlers do annotated handler
    // para achar o handler correto
    // isso é muito custoso?
    private final List<MessageHandler<? super Command>> commandHandlers;
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
        this.commandHandlers = builder.commandHandlers;
        this.interceptors = new ArrayList<>();
        this.dispatcherInterceptors = new ArrayList<>();
        this.defaultCommandCallback = builder.defaultCommandCallback;
        this.transactionManager = builder.transactionManager;
        this.rollbackConfiguration = builder.rollbackConfiguration;
    }

    @Override
    public void registerHandler(MessageHandler<? super Command> handler) {
        // o correto e receber o name do command aqui
        commandHandlers.add(handler);
    }

    @Override
    public void dispatch(final Command command) {
        dispatch(command, defaultCommandCallback);
    }

    @Override
    public <C, R> void dispatch(@Nonnull Command command, @Nonnull CommandCallBack<? super C, ? super R> callback) {
        log.debug("Dispatching command with callback");
        final var aHandler = commandHandlers
                .stream()
                .filter(ch -> ch.canHandle(command))
                .findFirst()
                .orElseThrow(() -> new NoHandlerForCommandException(command));

        handle(intercept(command), aHandler, callback);
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
            MessageHandler<? super Command> aHandler,
            CommandCallBack<C, R> callback
    ) {
        final var uow = DefaultUnitOfWork.startAndGet(command);
        uow.attachTransaction(transactionManager);

        InterceptorChain chain = new DefaultInterceptorChain<>(aHandler, interceptors, uow);

        final var aResult = uow.executeWithResult(chain::process,
                rollbackConfiguration);
        callback.onResult((C) command, (ResultMessage<? extends R>) aResult);
    }

    protected Command intercept(Command command) {
        Command interceptedCommand = command;
        for (MessageDispatcherInterceptor<? super Command> interceptor : dispatcherInterceptors) {
            interceptedCommand = (Command) interceptor.handle(interceptedCommand);
        }

        return interceptedCommand;
    }

    public void setRollbackConfiguration(@Nonnull RollbackConfiguration rollbackConfiguration) {
        this.rollbackConfiguration = rollbackConfiguration;
    }

    public static class Builder {
        private List<MessageHandler<? super Command>> commandHandlers;
        private TransactionManager transactionManager = NoTransactionManager.INSTANCE;
        private RollbackConfiguration rollbackConfiguration = RollbackConfigurationType.UNCHECKED_EXCEPTIONS;
        private CommandCallBack<Object, Object> defaultCommandCallback = LoggingCallback.INSTANCE;

        public Builder commandHandlers(List<MessageHandler<? super Command>> commandHandlers) {
            this.commandHandlers = commandHandlers == null ? new ArrayList<>() : commandHandlers;
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
