package com.kaua.event.driven.infrastructure.es.parameters;

import java.lang.invoke.MethodHandle;

@FunctionalInterface
public interface ParameterFactory {

    ParameterResolver create(MethodHandle executable, Class<?>[] parameters, int parameterIndex);
}
