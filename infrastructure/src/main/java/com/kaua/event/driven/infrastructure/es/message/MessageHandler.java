package com.kaua.event.driven.infrastructure.es.message;

public interface MessageHandler<T> {

    default <M> boolean canHandle(M message) {
        return true;
    }

    Object handle(T message) throws Exception;
}
