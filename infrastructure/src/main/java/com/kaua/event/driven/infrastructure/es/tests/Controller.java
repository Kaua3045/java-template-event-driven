package com.kaua.event.driven.infrastructure.es.tests;

import com.kaua.event.driven.infrastructure.es.command.CommandGateway;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventProcessor;
import com.kaua.event.driven.infrastructure.es.tests.values.CreateOrderCommand;
import com.kaua.event.driven.infrastructure.es.tests.values.CreatePaymentCommand;
import com.kaua.event.driven.infrastructure.es.tests.values.UpdateOrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hello")
public class Controller {

    @Autowired
    private CommandGateway commandGateway;

    @Autowired
    private EventProcessor eventProcessor;

    @PostMapping
    public String hello() {
        commandGateway.send(new CreateOrderCommand("123"));
        return "Hello";
    }

    @PostMapping("/payment")
    public String payment() {
        commandGateway.send(new CreatePaymentCommand("payment-confirmed"));
        return "Payment";
    }

    @PostMapping("/world")
    public String world(@RequestParam String id) {
        commandGateway.send(new UpdateOrderCommand(id, "456"));
        return "World";
    }

    @PostMapping("/reset")
    public String reset() {
        eventProcessor.resetTokens();
        return "Reset";
    }
}
