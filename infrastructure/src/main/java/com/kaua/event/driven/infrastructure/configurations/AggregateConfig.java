package com.kaua.event.driven.infrastructure.configurations;

import com.kaua.event.driven.infrastructure.cache.Cache;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateRoot;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.CacheEventSourcingRepository;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.CommandHandlerImpl;
import com.kaua.event.driven.infrastructure.es.command.impl.SimpleCommandBus;
import com.kaua.event.driven.infrastructure.es.interceptors.ValidationMessageInterceptor;
import com.kaua.event.driven.infrastructure.es.jpa.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.NullLockFactory;
import com.kaua.event.driven.infrastructure.uow.TransactionManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.stream.Collectors;

@Configuration
// TODO refactor this
public class AggregateConfig {

    @Bean
    public Map<Class<?>, AggregateModel<?>> aggregateModels(final ApplicationContext context) {
        return context.getBeansWithAnnotation(AggregateRoot.class)
                .values().stream()
                .collect(Collectors.toMap(
                        Object::getClass, // Use the aggregate root class as the key
                        v -> new DefaultAggregateModel<>(v.getClass()) // Create a new DefaultAggregateModel for each aggregate root
                ));
    }

    @Bean
    public Map<Class<?>, CacheEventSourcingRepository<?>> eventSourcingRepositoryMap(
            EventStore eventStore,
            Cache cache,
            Map<Class<?>, AggregateModel<?>> aggregateModels
    ) {
        return aggregateModels.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new CacheEventSourcingRepository<>(eventStore,
                                NullLockFactory.INSTANCE, entry.getValue(), cache))
                );
    }

    @Bean
    public Map<Class<?>, CommandHandlerImpl<?>> commandHandlers(
            Map<Class<?>, CacheEventSourcingRepository<?>> eventSourcingRepositoryMap
    ) {
        return eventSourcingRepositoryMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new CommandHandlerImpl<>(entry.getValue())
                ));
    }

    @Bean
    public CommandBus commandBus(
            Map<Class<?>, CommandHandlerImpl<?>> commandHandlers,
            Map<Class<?>, AggregateModel<?>> aggregateModels,
            TransactionManager transactionManager
    ) {
        final var aCommandBus = SimpleCommandBus.builder()
                .aggregates(aggregateModels)
                .commandHandlers(commandHandlers)
                .transactionManager(transactionManager)
                .build();

        aCommandBus.registerHandlerInterceptor(new ValidationMessageInterceptor<>());

        return aCommandBus;
    }
}
