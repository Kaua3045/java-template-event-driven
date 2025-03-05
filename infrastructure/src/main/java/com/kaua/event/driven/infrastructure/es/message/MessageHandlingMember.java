package com.kaua.event.driven.infrastructure.es.message;

/*
* Interface to describe a handler for specific message type
*
* @param <T> the type of the target object
* */
public interface MessageHandlingMember<T> {

    <M> boolean canHandle(M message);

    <M> Object handle(M message, T target);
}
