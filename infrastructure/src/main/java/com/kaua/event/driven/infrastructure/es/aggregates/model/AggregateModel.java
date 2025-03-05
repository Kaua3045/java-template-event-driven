package com.kaua.event.driven.infrastructure.es.aggregates.model;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;

import java.util.List;
import java.util.Map;

public interface AggregateModel<T> {

    Map<Class<?>, MessageHandlingMember<? super T>> commandHandlers();

    Map<Class<?>, List<MessageHandlingMember<? super T>>> commandHandlersInterceptors();

    Map<Class<?>, MessageHandlingMember<? super T>> eventHandlers();

    Object getAggregateIdentifier(T aggregate);

    Long getAggregateVersion(T aggregate);

    Class<? extends T> entityClass();

    void publish(T aggregateRoot, DomainEvent event);
}
