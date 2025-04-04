package com.kaua.event.driven.infrastructure.es.inspector;

import com.kaua.event.driven.infrastructure.es.aggregates.annotations.CommandHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventSourcingHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.ResetHandler;
import com.kaua.event.driven.infrastructure.es.message.AnnotatedMessageHandling;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;
import com.kaua.event.driven.infrastructure.es.parameters.ParameterFactory;
import com.kaua.event.driven.infrastructure.es.parameters.SimpleParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnnotationHandlerInspector<T> {

    private static final Logger log = LoggerFactory.getLogger(AnnotationHandlerInspector.class);
    private final Class<T> inspectedType;
    private final ParameterFactory parameterFactory;
    private final Map<Class<?>, AnnotationHandlerInspector<?>> registry;
    private final Map<Class<?>, List<MessageHandlingMember<? super T>>> handlers;

    private AnnotationHandlerInspector(
            Class<T> inspectedType,
            ParameterFactory parameterFactory,
            Map<Class<?>, AnnotationHandlerInspector<?>> registry
    ) {
        this.inspectedType = inspectedType;
        this.parameterFactory = parameterFactory;
        this.registry = registry;
        this.handlers = new HashMap<>();
    }

    public static <T> AnnotationHandlerInspector<T> inspect(
            Class<? extends T> inspectedType
    ) {
        return create(inspectedType, new SimpleParameterResolver(), new HashMap<>());
    }

    private static <T> AnnotationHandlerInspector<T> create(
            Class<? extends T> inspectedType,
            ParameterFactory parameterFactory,
            Map<Class<?>, AnnotationHandlerInspector<?>> registry
    ) {
        if (!registry.containsKey(inspectedType)) {
            System.out.println("Creating new inspector for " + inspectedType);
            registry.put(
                    inspectedType,
                    AnnotationHandlerInspector.initialize(
                            (Class<T>) inspectedType,
                            parameterFactory,
                            registry
                    )
            );
        }

        return (AnnotationHandlerInspector<T>) registry.get(inspectedType);
    }

    private static <T> AnnotationHandlerInspector<T> initialize(
            Class<T> inspectedType,
            ParameterFactory parameterFactory,
            Map<Class<?>, AnnotationHandlerInspector<?>> registry
    ) {

        AnnotationHandlerInspector<T> inspector = new AnnotationHandlerInspector<>(
                inspectedType,
                parameterFactory,
                registry
        );

        inspector.initializeMessageHandlers(
                parameterFactory
        );

        return inspector;
    }

    private void initializeMessageHandlers(ParameterFactory parameterFactory) {
        handlers.put(inspectedType, new ArrayList<>());

        MethodHandles.Lookup lookup = MethodHandles.lookup();
        for (Method method : inspectedType.getDeclaredMethods()) {
            try {
                if (method.isAnnotationPresent(CommandHandler.class)
                        || method.isAnnotationPresent(EventSourcingHandler.class)
                        || method.isAnnotationPresent(EventHandler.class)
                        || method.isAnnotationPresent(ResetHandler.class)
                ) {
                    MethodHandle methodHandle = lookup.findVirtual(
                            inspectedType, method.getName(),
                            MethodType.methodType(method.getReturnType(), method.getParameterTypes())
                    );

                    handlers.get(inspectedType).add(
                            new AnnotatedMessageHandling<>(
                                    methodHandle,
                                    methodHandle.type().parameterCount() == 1
                                            ? Object.class
                                            : methodHandle.type().parameterType(1), // TODO verify this, add tests to this part
                                    parameterFactory,
                                    Arrays.asList(method.getAnnotations())
                            )
                    );

                    log.debug("Method {} is annotated with [{}] and added to handlers", method.getName(),
                            Arrays.stream(method.getAnnotations())
                                    .map(Objects::toString)
                                    .collect(Collectors.joining(", ")));
                }
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Stream<MessageHandlingMember<? super T>> getHandlers(Class<?> type) {
        return handlers.getOrDefault(type, Collections.emptyList()).stream();
    }
}
