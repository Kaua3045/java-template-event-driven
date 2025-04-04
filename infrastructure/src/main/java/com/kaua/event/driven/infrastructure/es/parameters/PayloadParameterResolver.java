package com.kaua.event.driven.infrastructure.es.parameters;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;

public class PayloadParameterResolver implements ParameterResolver {

    private final Class<?> payloadType;

    public PayloadParameterResolver(Class<?> payloadType) {
        this.payloadType = payloadType;
    }

    @Override
    public Object resolve(Object value) {
        System.out.println("PayloadParameterResolver.resolve " + value.getClass());

        if (value instanceof TrackedEventMessage<?>) {
            return ((TrackedEventMessage<?>) value).getPayload();
        } else if (value instanceof Command) {
            return value;
        } else if (value instanceof DomainEvent) {
            return value;
        } else {
            throw new IllegalArgumentException("Unsupported payload type: " + value.getClass());
        }
    }

    @Override
    public boolean matches(Object value) {
        return value != null && payloadType.equals(value.getClass());
    }

    @Override
    public Class<?> supportedPayloadType() {
        return payloadType;
    }
}
