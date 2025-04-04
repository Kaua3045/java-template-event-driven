package com.kaua.event.driven;

import com.kaua.event.driven.fixture.AggregateFixture;
import com.kaua.event.driven.infrastructure.es.tests.OrderAggregate;
import com.kaua.event.driven.infrastructure.es.tests.values.CreateOrderCommand;
import com.kaua.event.driven.infrastructure.es.tests.values.OrderCreatedEvent;
import org.junit.jupiter.api.Test;

public class OrderAggregateTest {

    @Test
    void testOrderAggregate() {
        AggregateFixture<OrderAggregate> fixture = new AggregateFixture<>(OrderAggregate.class);

        final var aCommand = new CreateOrderCommand("teste");
        final var aEvent = new OrderCreatedEvent(
                aCommand.aggregateId(),
                aCommand.description(),
                aCommand.aggregateVersion()
        );

        fixture.givenNoPriorActivity()
                .when(aCommand)
                .expectSuccessfulHandlerExecution()
                .expectEvents(aEvent);
//                .expectEvents(new OrderUpdatedEvent(
//                        aCommand.aggregateId(),
//                        aCommand.description(),
//                        aCommand.aggregateVersion()
//                ));

//        fixture.printStoredEvents();
    }
}
