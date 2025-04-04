package com.kaua.event.driven.infrastructure.es.parameters;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;

import java.lang.invoke.MethodHandle;

public class SimpleParameterResolver implements ParameterFactory {

    @Override
    public ParameterResolver create(
            final MethodHandle executable,
            final Class<?>[] parameters,
            final int parameterIndex
    ) {
        Class<?> parameterType = parameters[parameterIndex];

        if (DomainEvent.class.isAssignableFrom(parameterType)) {
            return new PayloadParameterResolver(parameterType);
        } else if (Command.class.isAssignableFrom(parameterType)) {
            return new PayloadParameterResolver(parameterType);
        } else if (TrackedEventMessage.class.equals(parameterType)) {
            throw new UnsupportedOperationException("TrackedEventMessage is not supported as parameter type.");
        }

        return null;
    }
}
