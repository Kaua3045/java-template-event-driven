package com.kaua.event.driven.infrastructure.es.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;

public class AnnotatedMessageHandling<T> implements MessageHandlingMember<T> {

    private static final Logger log = LoggerFactory.getLogger(AnnotatedMessageHandling.class);

    private final MethodHandle methodHandle;
    private final Class<T> commandClass;

    public AnnotatedMessageHandling(MethodHandle methodHandle) {
        this.methodHandle = methodHandle;
        this.commandClass = (Class<T>) methodHandle.type().parameterArray()[1];
    }

    @Override
    public <M> boolean canHandle(M message) {
        log.debug("Checking if can handle message {}", message);
        return commandClass.isAssignableFrom(message.getClass());
    }

    @Override
    public <M> Object handle(M message, T target) {
        try {
            log.debug("Handling message {} with target {}", message, target);
            // TODO correto seria verificar os params do methodHandle e adicionar todos
            return methodHandle.invoke(target, message);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
