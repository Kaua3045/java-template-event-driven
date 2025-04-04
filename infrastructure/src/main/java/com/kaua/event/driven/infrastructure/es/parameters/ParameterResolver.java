package com.kaua.event.driven.infrastructure.es.parameters;

public interface ParameterResolver<T> {

    T resolve(Object value);

    boolean matches(Object value);

    default Class<?> supportedPayloadType() {
        return Object.class;
    }
}
