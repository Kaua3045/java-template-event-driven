package com.kaua.event.driven.infrastructure.es.command.callback;

import com.kaua.event.driven.infrastructure.uow.ResultMessage;

@FunctionalInterface
public interface CommandCallBack<C, R> {

    void onResult(C command, ResultMessage<? extends R> result);
}
