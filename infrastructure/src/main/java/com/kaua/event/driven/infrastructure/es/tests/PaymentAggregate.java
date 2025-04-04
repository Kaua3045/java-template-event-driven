package com.kaua.event.driven.infrastructure.es.tests;

import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.*;
import com.kaua.event.driven.infrastructure.es.tests.values.CreatePaymentCommand;
import com.kaua.event.driven.infrastructure.es.tests.values.PaymentCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AggregateRoot
public class PaymentAggregate {

    private static final Logger log = LoggerFactory.getLogger(PaymentAggregate.class);

    @TargetAggregateIdentifier
    private String id;

    @AggregateVersion
    private Long version;

    private String text;

    public PaymentAggregate() {
    }

    @CommandHandler
    public void handle(CreatePaymentCommand command) {
        log.debug("Handling CreatePaymentCommand: {}", command);
        AggregateLifecycle.apply(
                new PaymentCreatedEvent(
                        command.aggregateId(),
                        command.text(),
                        command.aggregateVersion()
                )
        );
    }

    @EventSourcingHandler
    public void on(PaymentCreatedEvent event) {
        log.debug("Handling PaymentCreatedEvent: {}", event);
        this.id = event.aggregateId();
        this.version = event.aggregateVersion();
        this.text = event.text();
    }

    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public String getText() {
        return text;
    }
}
