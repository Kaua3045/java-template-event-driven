package com.kaua.event.driven.infrastructure.es.eventbus;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractEventBus implements EventBus {

    private static final Logger log = LoggerFactory.getLogger(AbstractEventBus.class);

    private final Map<Class<?>, AggregateModel<?>> aggregates;

    protected AbstractEventBus(Map<Class<?>, AggregateModel<?>> aggregates) {
        this.aggregates = aggregates;
    }

    @Override
    public void publish(DomainEvent event) {
        handle(event);
    }

    public void handle(DomainEvent event) {
        // TODO aqui tecnicamente deveriamos pegar todos os handlers desse event
        // e mandar o event para cada um deles
        // se 1 falhar, todos falham e nem deve salvar dai no database
        Class<?> clazz = getAggregateClassForEvent(event);
        if (clazz == null) {
            throw new NoHandlerForEventException(event);
        }

        AggregateModel<?> model = aggregates.get(clazz);
        if (model == null) {
            throw NotFoundException.withMessage("Aggregate model not found for class %s and event %s"
                    .formatted(clazz.getSimpleName(), event.getClass().getSimpleName()));
        }

        log.debug("Publishing event {} for other handlers", event);
    }

    private Class<?> getAggregateClassForEvent(DomainEvent event) {
        return aggregates.entrySet().stream()
                .filter(entry ->
                        entry.getValue().eventHandlers().containsKey(event.getClass()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }
}
