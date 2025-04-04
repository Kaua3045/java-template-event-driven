package com.kaua.event.driven.infrastructure.es.tests;

import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.*;
import com.kaua.event.driven.infrastructure.es.tests.values.CreateOrderCommand;
import com.kaua.event.driven.infrastructure.es.tests.values.OrderCreatedEvent;
import com.kaua.event.driven.infrastructure.es.tests.values.OrderUpdatedEvent;
import com.kaua.event.driven.infrastructure.es.tests.values.UpdateOrderCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AggregateRoot
public class OrderAggregate {

    private static final Logger log = LoggerFactory.getLogger(OrderAggregate.class);

    @TargetAggregateIdentifier
    private String id;

    @AggregateVersion
    private Long version;

    private String description;

    public OrderAggregate() {
    }

    @CommandHandler
    public void handle(CreateOrderCommand command) {
        log.debug("Handling CreateOrderCommand: {}", command);
        AggregateLifecycle.apply(
                new OrderCreatedEvent(
                        command.aggregateId(),
                        command.description(),
                        command.aggregateVersion()
                )
        );
    }

    @CommandHandler
    public void handle(UpdateOrderCommand command) {
        log.debug("Handling UpdateOrderCommand: {}", command);
        AggregateLifecycle.apply(
                new OrderUpdatedEvent(
                        command.aggregateId(),
                        command.description(),
                        getVersion() // aqui pega a versão atual do aggregate
                )
        );
    }

    @EventSourcingHandler
    public void on(OrderCreatedEvent event) {
        log.debug("Handling OrderCreatedEvent: {}", event);
        this.id = event.aggregateId();
        this.version = event.aggregateVersion();
        this.description = event.description();
    }

    @EventSourcingHandler
    public void on(OrderUpdatedEvent event) {
        log.debug("Handling OrderUpdatedEvent: {}", event);
        this.id = event.aggregateId();
        this.version = event.aggregateVersion();
        this.description = event.description();
    }

    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }
}
