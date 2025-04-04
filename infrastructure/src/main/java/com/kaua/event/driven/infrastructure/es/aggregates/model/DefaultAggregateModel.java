package com.kaua.event.driven.infrastructure.es.aggregates.model;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.*;
import com.kaua.event.driven.infrastructure.es.message.AnnotatedMessageHandling;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultAggregateModel<T> implements AggregateModel<T> {

    private final Map<Class<?>, List<MessageHandlingMember<? super T>>> commandHandlersInterceptors = new ConcurrentHashMap<>();
    private final Map<Class<?>, MessageHandlingMember<? super T>> commandHandlers = new ConcurrentHashMap<>();
    private final Map<Class<?>, MessageHandlingMember<? super T>> eventHandlers = new ConcurrentHashMap<>();
    private final MethodHandle aggregateIdentifier;
    private final MethodHandle aggregateVersion;
    private final Class<T> entityClass;

    public DefaultAggregateModel(Class<T> aggregateRoot) {
        if (aggregateRoot == null) {
            throw new InternalErrorException("Aggregate root class cannot be null");
        }

        this.entityClass = aggregateRoot;

        this.aggregateIdentifier = findFieldByAnnotation(
                aggregateRoot,
                TargetAggregateIdentifier.class
        );
        this.aggregateVersion = findFieldByAnnotation(
                aggregateRoot,
                AggregateVersion.class
        );

        findMethodHandlersByAnnotation(aggregateRoot, CommandHandler.class, commandHandlers);
        findMethodHandlersByAnnotation(aggregateRoot, EventSourcingHandler.class, eventHandlers);
        findMethodHandlersInterceptorByAnnotation(aggregateRoot, CommandHandlerInterceptor.class, commandHandlersInterceptors);
    }

    @Override
    public Map<Class<?>, MessageHandlingMember<? super T>> commandHandlers() {
        return commandHandlers;
    }

    @Override
    public Map<Class<?>, List<MessageHandlingMember<? super T>>> commandHandlersInterceptors() {
        return commandHandlersInterceptors;
    }

    @Override
    public Map<Class<?>, MessageHandlingMember<? super T>> eventHandlers() {
        return eventHandlers;
    }

    @Override
    public Object getAggregateIdentifier(T aggregate) {
        try {
            return aggregateIdentifier.invoke(aggregate);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long getAggregateVersion(T aggregate) {
        try {
            return (Long) aggregateVersion.invoke(aggregate);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<? extends T> entityClass() {
        return entityClass;
    }

    @Override
    public void publish(T aggregateRoot, DomainEvent event) {
        MessageHandlingMember<? super T> methodHandle = eventHandlers.values()
                .stream()
                .filter(h -> h.canHandle(event))
                .findFirst()
                .orElseThrow(() -> new NoHandlerForEventException(event));

        methodHandle.handle(event, aggregateRoot);
    }

    private void findMethodHandlersByAnnotation(
            Class<T> aggregateRoot,
            Class<? extends Annotation> annotation,
            Map<Class<?>, MessageHandlingMember<? super T>> handlers
    ) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        for (Method method : aggregateRoot.getDeclaredMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                try {
                    MethodHandle methodHandle = lookup.findVirtual(
                            aggregateRoot, method.getName(),
                            MethodType.methodType(method.getReturnType(), method.getParameterTypes())
                    );
                    handlers.put(method.getParameterTypes()[0],
                            new AnnotatedMessageHandling<>(
                                    methodHandle,
                                    method.getParameterTypes()[0]
                            ));
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void findMethodHandlersInterceptorByAnnotation(
            Class<T> aggregateRoot,
            Class<? extends Annotation> annotation,
            Map<Class<?>, List<MessageHandlingMember<? super T>>> handlers
    ) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        for (Method method : aggregateRoot.getDeclaredMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                try {
                    MethodHandle methodHandle = lookup.findVirtual(
                            aggregateRoot, method.getName(),
                            MethodType.methodType(method.getReturnType(), method.getParameterTypes())
                    );
                    List<MessageHandlingMember<? super T>> interceptors = new ArrayList<>();
                    interceptors.add(new AnnotatedMessageHandling<>(
                            methodHandle,
                            method.getParameterTypes()[0]
                    ));

                    handlers.put(method.getParameterTypes()[0], interceptors);
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private MethodHandle findFieldByAnnotation(
            Class<T> aggregateRoot,
            Class<? extends Annotation> annotation
    ) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle field = null;
        int count = 0;

        for (Field f : aggregateRoot.getDeclaredFields()) {
            if (f.isAnnotationPresent(annotation)) {
                f.setAccessible(true);
                try {
                    field = lookup.unreflectGetter(f);
                    count++;
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (count > 1) {
            throw new InternalErrorException("Only one field with annotation %s is allowed".formatted(annotation.getSimpleName()));
        }

        return field;
    }
}
