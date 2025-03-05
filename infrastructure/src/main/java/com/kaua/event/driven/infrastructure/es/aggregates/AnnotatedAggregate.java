package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.exceptions.NoHandlerForCommandException;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;
import com.kaua.event.driven.infrastructure.es.interceptors.AnnotatedCommandHandlerInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.DefaultInterceptorChain;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;
import com.kaua.event.driven.infrastructure.es.scope.ScopeDescriptor;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AnnotatedAggregate<T> extends AggregateLifecycle implements Aggregate<T>, ApplyMore {

    private static final Logger log = LoggerFactory.getLogger(AnnotatedAggregate.class);

    private final AggregateModel<T> model;
    private final EventBus eventBus;
    private T aggregateRoot;
    private boolean isDeleted = false;

    public AnnotatedAggregate(
            AggregateModel<T> model,
            EventBus eventBus,
            T aggregateRoot
    ) {
        this.model = model;
        this.eventBus = eventBus;
        this.aggregateRoot = aggregateRoot;
    }

    @Override
    protected <T> ApplyMore doApply(T payload) {
        publish(payload);
        return this;
    }

    public void publish(Object message) {
        model.publish(aggregateRoot, (DomainEvent) message);
        publishOnEventBus(message);
    }

    public void publishOnEventBus(Object message) {
        if (eventBus != null) {
            eventBus.publish((DomainEvent) message);
        }
    }

    @Override
    protected boolean getIsLive() {
        return true;
    }

    @Override
    public String type() {
        return aggregateRoot.getClass().getName();
    }

    @Override
    public Object identifier() {
        return model.getAggregateIdentifier(aggregateRoot);
    }

    @Override
    public Long version() {
        return model.getAggregateVersion(aggregateRoot);
    }

    @Override
    protected void doMarkDeleted() {
        this.isDeleted = true;
    }

    @Override
    public Object handle(Object message) {
        log.debug("Handling message: {}", message);
        Callable<Object> messageHandling;

        if (message instanceof Command) {
            messageHandling = () -> handle((Command) message);
        } else if (message instanceof DomainEvent) {
            messageHandling = () -> handle((DomainEvent) message);
        } else {
            throw new InternalErrorException("Message type not supported: " + message.getClass().getName());
        }

        try {
            return executeWithResult(messageHandling);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object handle(Command message) {
        // TODO verificar esse métddo

        List<AnnotatedCommandHandlerInterceptor<? super T>> interceptors =
                model.commandHandlersInterceptors()
                        .values()
                        .stream()
                        .map(list -> list.stream()
                                .filter(interceptor -> interceptor.canHandle(message))
                                .map(interceptor -> new AnnotatedCommandHandlerInterceptor<>(interceptor, aggregateRoot))
                                .toList())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        MessageHandlingMember<? super T> potentialHandler = model.commandHandlers()
                .values()
                .stream()
                .filter(member -> member.canHandle(message))
                .findFirst()
                .orElseThrow(() -> new NoHandlerForCommandException(message));

        if (interceptors.isEmpty()) {
            return potentialHandler.handle(message, aggregateRoot);
        }

        try {
            return new DefaultInterceptorChain<>(
                    m -> potentialHandler,
                    interceptors,
                    (UnitOfWork<? extends Command>) CurrentUnitOfWork.get()
            ).process();
        } catch (Exception e) {
            log.error("Error handling command", e);
            throw new RuntimeException(e);
        }

    }

    private Object handle(DomainEvent event) {
        model.publish(aggregateRoot, event);
        return null;
    }

    @Override
    public <R> R invoke(Function<T, R> invocation) {
        try {
            return executeWithResult(() -> invocation.apply(aggregateRoot));
        } catch (Exception e) {
            log.error("Error invoking function", e);
            throw new InternalErrorException("Exception occurred while invoking an aggregate", e);
        }
    }

    @Override
    public void execute(Consumer<T> invocation) {
        execute(() -> invocation.accept(aggregateRoot));
    }

    @Override
    public boolean isDeleted() {
        return isDeleted;
    }

    @Override
    public Class<? extends T> rootType() {
        return (Class<? extends T>) aggregateRoot.getClass();
    }

    public T getAggregateRoot() {
        return aggregateRoot;
    }

    @Override
    public ApplyMore andThenApply(Supplier<?> payloadOrMessageSupplier) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ApplyMore andThen(Runnable runnable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ApplyMore andThenIf(Supplier<Boolean> condition, Runnable runnable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ScopeDescriptor describeScope() {
        return new AggregateScopeDescriptor(type(), identifier());
    }
}
