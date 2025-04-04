package com.kaua.event.driven.infrastructure.es.message;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;
import com.kaua.event.driven.infrastructure.es.parameters.FixedParameterResolver;
import com.kaua.event.driven.infrastructure.es.parameters.ParameterFactory;
import com.kaua.event.driven.infrastructure.es.parameters.ParameterResolver;
import com.kaua.event.driven.infrastructure.es.parameters.SimpleParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.util.*;
import java.util.stream.Collectors;

public class AnnotatedMessageHandling<T> implements MessageHandlingMember<T> {

    private static final Logger log = LoggerFactory.getLogger(AnnotatedMessageHandling.class);

    private final MethodHandle methodHandle;
    private final Class<?> payloadType;
    private final int parameterCount;
    private final ParameterResolver<?>[] parameterResolvers;
    private final List<Annotation> annotations;

    public AnnotatedMessageHandling(
            MethodHandle methodHandle,
            Class<?> explicitPayloadType
    ) {
        this(methodHandle, explicitPayloadType, new SimpleParameterResolver(), Collections.emptyList());
    }

    public AnnotatedMessageHandling(
            MethodHandle methodHandle,
            Class<?> explicitPayloadType,
            List<Annotation> annotations
    ) {
        this(methodHandle, explicitPayloadType, new SimpleParameterResolver(), annotations);
    }

    public AnnotatedMessageHandling(
            MethodHandle methodHandle,
            Class<?> explicitPayloadType,
            ParameterFactory parameterResolverFactory,
            List<Annotation> annotations
    ) {
        this.methodHandle = methodHandle;
        this.parameterCount = methodHandle.type().parameterCount();
        this.annotations = annotations;

        Class<?>[] parameters = methodHandle.type().parameterArray();
        this.parameterResolvers = new ParameterResolver[parameterCount];
        Class<?> supportedPayloadType = explicitPayloadType;

        for (int i = 1; i < parameterCount; i++) { // start in 1 to skip the target object
            parameterResolvers[i - 1] = parameterResolverFactory.create(methodHandle, parameters, i);
            if (parameterResolvers[i - 1] == null) {
                log.error("Unable to resolve parameter {} ({}) in handler {}.", i, parameters[i].getSimpleName(), methodHandle);

                throw new InternalErrorException(
                        "Unable to resolve parameter " + i + " (" + parameters[i].getSimpleName() +
                                ") in handler " + methodHandle + ".");
            }
            if (supportedPayloadType.equals(parameterResolvers[i - 1].supportedPayloadType())) {
                supportedPayloadType = parameterResolvers[i - 1].supportedPayloadType();
            } else if (!(parameterResolvers[i - 1] instanceof FixedParameterResolver) &&
                    !parameterResolvers[i - 1].supportedPayloadType().equals(supportedPayloadType)) {
                log.error("The method {} seems to have parameters that put conflicting requirements on the payload type applicable on that method: {} vs {}",
                        methodHandle, supportedPayloadType, parameterResolvers[i - 1].supportedPayloadType());

                throw new InternalErrorException(String.format(
                        "The method %s seems to have parameters that put conflicting requirements on the payload type" +
                                " applicable on that method: %s vs %s", methodHandle,
                        supportedPayloadType, parameterResolvers[i - 1].supportedPayloadType()));
            }
        }

        this.payloadType = supportedPayloadType;
    }

    @Override
    public <M> boolean canHandle(M message) {
        log.debug("Checking if can handle message {}", message);
        switch (message) {
            case TrackedEventMessage<?> trackedEventMessage -> {
                log.debug("Checking if can handle tracked event {} and payload to check {}", trackedEventMessage.getPayload().getClass().getSimpleName(), payloadType.getSimpleName());
                return payloadType.equals(trackedEventMessage.getPayload().getClass());
            }
            case Command command -> {
                log.debug("Checking if can handle command {} and payload to check {}", command.getClass().getSimpleName(), payloadType.getSimpleName());
                return payloadType.equals(command.getClass());
            }
            case DomainEvent domainEvent -> {
                log.debug("Checking if can handle domain event {} and payload to check {}", domainEvent.getClass().getSimpleName(), payloadType.getSimpleName());
                return payloadType.equals(domainEvent.getClass());
            }
            default -> {
                return false;
            }
        }
    }

    @Override
    public <M> Object handle(M message, T target) {
        try {
            if (message == null) {
                return methodHandle.invoke(target);
            }

            switch (message) {
                case TrackedEventMessage<?> trackedEventMessage -> {
                    log.debug("Handling tracked event [payload: {}] and tracking token [token: {}] with target {}",
                            trackedEventMessage.getPayload(), trackedEventMessage.trackingToken(), target);
                    final var parameters = Arrays.stream(resolveParameters(trackedEventMessage.getPayload()))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    if (!parameters.isEmpty()) {
                        parameters.addFirst(target);
                        log.debug("Invoking tracked event method handle with parameters {}", parameters.toArray());
                        return methodHandle.invokeWithArguments(parameters);
                    }

                    log.debug("No parameters to resolve in tracked event method handle, invoking method handle with target {}", target);
                    return methodHandle.invoke(target);
                }
                case Command command -> {
                    log.debug("Handling command {} with target {}", command, target);
                    final var parameters = Arrays.stream(resolveParameters(command))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    if (!parameters.isEmpty()) {
                        parameters.addFirst(target);
                        log.debug("Invoking command method handle with parameters {}", parameters.toArray());
                        return methodHandle.invokeWithArguments(parameters);
                    }

                    log.debug("No parameters to resolve in command method handle, invoking method handle with target {}", target);
                    return methodHandle.invoke(target);
                }
                case DomainEvent domainEvent -> {
                    log.debug("Handling domain event {} with target {}", domainEvent, target);
                    final var parameters = Arrays.stream(resolveParameters(domainEvent))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    if (!parameters.isEmpty()) {
                        parameters.addFirst(target);
                        log.debug("Invoking domain event method handle with parameters {}", parameters.toArray());
                        return methodHandle.invokeWithArguments(parameters);
                    }

                    log.debug("No parameters to resolve in domain event method handle, invoking method handle with target {}", target);
                    return methodHandle.invoke(target);
                }
                default -> log.warn("Received unsupported message type {}", message.getClass().getSimpleName());
            }
        } catch (Throwable e) {
            throw new InternalErrorException("Error handling message", e);
        }
        throw new InternalErrorException("Unsupported message type " + message.getClass().getSimpleName());
    }

    @Override
    public boolean hasAnnotation(Class<?> annotationType) {
        log.debug("Checking if handler has annotation {}", annotationType.getSimpleName());
        return this.annotations
                .stream()
                .anyMatch(it -> it.annotationType().equals(annotationType));
    }

    @Override
    public <A extends Annotation> Optional<A> getAnnotation(Class<A> annotationType) {
        log.debug("Getting annotation {} from handler", annotationType.getSimpleName());
        return (Optional<A>) this.annotations.stream()
                .filter(it -> it.annotationType().equals(annotationType))
                .findFirst();
    }

    private Object[] resolveParameters(Object message) {
        Object[] parameters = new Object[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            if (parameterResolvers[i] != null) {
                parameters[i] = parameterResolvers[i].resolve(message);
                log.debug("Resolved parameter {} with value {}", i, parameters[i]);
            }
        }
        return parameters;
    }
}
