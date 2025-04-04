package com.kaua.event.driven.infrastructure.es.tests;

import com.kaua.event.driven.infrastructure.es.command.CommandGateway;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.tracking.TrackingProcessor;
import com.kaua.event.driven.infrastructure.es.eventstore.jpa.EventJpaRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class CmdRunner implements CommandLineRunner {

    private final CommandGateway commandGateway;
    private final EventHandlerInvoker eventHandlerInvoker;
    private final EventJpaRepository eventJpaRepository;
    private final TrackingProcessor trackingProcessor;

    private final ExecutorService executors = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual()
                    .name("event-handler")
                    .factory()
    );

    public CmdRunner(
            CommandGateway commandGateway,
            EventHandlerInvoker eventHandlerInvoker,
            EventJpaRepository eventJpaRepository,
            TrackingProcessor trackingProcessor
    ) {
        this.commandGateway = commandGateway;
        this.eventHandlerInvoker = eventHandlerInvoker;
        this.eventJpaRepository = eventJpaRepository;
        this.trackingProcessor = trackingProcessor;
    }

    @Override
    public void run(String... args) throws Exception {
        trackingProcessor.start();
//        executors.submit(trackingProcessor::start);
        // TODO o gargalo foi o db quando usamos o async
        // foram processadas 469 por segundo, mas o db deu timeout
//        long COMMAND_COUNT = 500; // 5 hundred thousand
//        long COMMAND_COUNT = 2 * 1000 * 100; // 5 hundred thousand
//        commandGateway.send(new CreateOrderCommand("ready,"));
//        long t1 = System.currentTimeMillis();
//        for (int t = 0; t < COMMAND_COUNT; t++) {
//            commandGateway.send(new CreatePaymentCommand("go! " + t));
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.printf("Just did %d commands per second%n", ((COMMAND_COUNT * 1000) / (t2 - t1)));
    }

    @Scheduled(initialDelay = 10, fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    public void run() {
//        trackingProcessor.start();
//        eventJpaRepository.findAll()
//                .forEach(it -> {
//                    try {
//                        final var aEvent = (DomainEvent) Json.readValue(it.getData(), Class.forName(it.getType()));
//                        eventHandlerInvoker.handle(aEvent);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                });
    }
}

