package com.kaua.event.driven.infrastructure.es.command;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageDispatcherInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import jakarta.annotation.Nonnull;

public interface CommandBus {

    void dispatch(Command command);

    <C, R> void dispatch(
            @Nonnull Command command,
            @Nonnull CommandCallBack<? super C, ? super R> callback
    );

    void registerHandler(MessageHandler<? super Command> handler);

    void registerHandlerInterceptor(MessageHandlerInterceptor<? super Command> interceptor);

    void registerDispatcherInterceptor(MessageDispatcherInterceptor<? super Command> interceptor);
}
