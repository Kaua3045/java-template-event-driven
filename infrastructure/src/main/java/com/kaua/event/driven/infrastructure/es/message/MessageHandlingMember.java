package com.kaua.event.driven.infrastructure.es.message;

import java.lang.annotation.Annotation;
import java.util.Optional;

/*
* Interface to describe a handler for specific message type
*
* @param <T> the type of the target object
* */
public interface MessageHandlingMember<T> {

    <M> boolean canHandle(M message);

    <M> Object handle(M message, T target);

    boolean hasAnnotation(Class<?> annotationType);

    <A extends Annotation> Optional<A> getAnnotation(Class<A> annotationType);
}
