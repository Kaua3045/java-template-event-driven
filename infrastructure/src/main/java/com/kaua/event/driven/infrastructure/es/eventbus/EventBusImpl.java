package com.kaua.event.driven.infrastructure.es.eventbus;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.jpa.EventEntity;
import com.kaua.event.driven.infrastructure.es.jpa.EventJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

//@Component
public class EventBusImpl implements EventBus {

    private static final Logger log = LoggerFactory.getLogger(EventBusImpl.class);

    private final ApplicationContext context;
    private final EventJpaRepository eventJpaRepository;
    private final Map<Class<?>, AggregateModel<?>> aggregates;

    public EventBusImpl(
            ApplicationContext context,
            EventJpaRepository eventJpaRepository,
            Map<Class<?>, AggregateModel<?>> aggregates
    ) {
        this.context = context;
        this.eventJpaRepository = eventJpaRepository;
        this.aggregates = aggregates;
    }

    @Override
    public void publish(DomainEvent event) {
        // TODO aqui seria bom agrupar, ou tudo funciona, ou tudo falha
        log.debug("Publishing event, saving and handling: {}", event);
        // TODO aqui precisamos fazer um validate antes de salvar ou deixar o banco rejeitar
        eventJpaRepository.save(new EventEntity(
                event.eventId(),
                event.aggregateId(),
                event.getClass().getName(),
                Json.writeValueAsString(event),
                event.aggregateVersion()
        ));

        Class<?> clazz = getAggregateClassForEvent(event);
        if (clazz == null) {
            throw new NoHandlerForEventException(event);
        }

        AggregateModel<?> model = aggregates.get(clazz);
        if (model == null) {
            throw NotFoundException.withMessage("Aggregate model not found for class %s and event %s"
                    .formatted(clazz.getSimpleName(), event.getClass().getSimpleName()));
        }

        // TODO tecnicamente aqui não precisaria mais, o correto seria garantir o save, pois agora executamos direto no annotated
        // e aqui se torna repetido, o correto seria salvar aqui ou em outro lugar
        // isso de publicar direto no annotated e aqui invocar por exemplo as sagas, vai ser muito bom
        // Deveria mandar para o Kafka para o query model consumir e atualizar o banco de dados
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
