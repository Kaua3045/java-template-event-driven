package com.kaua.event.driven.infrastructure.es.parameters;

public class FixedParameterResolver<T> implements ParameterResolver<Object> {

    private final T fixed;

    public FixedParameterResolver(T fixed) {
        this.fixed = fixed;
    }

    @Override
    public Object resolve(Object value) {
        return fixed;
    }

    @Override
    public boolean matches(Object value) {
        return true;
    }
}
