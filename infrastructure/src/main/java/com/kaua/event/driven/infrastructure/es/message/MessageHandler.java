package com.kaua.event.driven.infrastructure.es.message;

public interface MessageHandler<T> {

    Object handle(T message) throws Exception;
}
