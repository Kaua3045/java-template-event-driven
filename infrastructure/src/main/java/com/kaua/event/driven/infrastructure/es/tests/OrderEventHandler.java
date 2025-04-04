package com.kaua.event.driven.infrastructure.es.tests;

import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AllowReplay;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.ResetHandler;
import com.kaua.event.driven.infrastructure.es.tests.values.OrderCreatedEvent;
import com.kaua.event.driven.infrastructure.es.tests.values.OrderUpdatedEvent;
import org.springframework.stereotype.Component;

@AnnotationTest
@Component
public class OrderEventHandler {

    @EventHandler
    @AllowReplay
    public void handleCreate(OrderCreatedEvent event) {
        System.out.println("Order created: " + event);
    }

    @EventHandler
    @AllowReplay(value = false)
    public void handleUpdate(OrderUpdatedEvent event) {
        System.out.println("Order updated: " + event);
    }

    @ResetHandler
    public void reset() {
        System.out.println("Resetting OrderEventHandler");
    }
}
