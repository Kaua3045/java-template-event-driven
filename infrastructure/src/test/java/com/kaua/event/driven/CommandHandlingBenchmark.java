package com.kaua.event.driven;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateRepository;
import com.kaua.event.driven.infrastructure.es.aggregates.AnnotatedAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventSourcingHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.TargetAggregateIdentifier;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.AbstractRepository;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.impl.AsyncCommandBus;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle.apply;

public class CommandHandlingBenchmark {

    private static final UUID aggregateIdentifier = UUID.randomUUID();

    public static void main(String[] args) {
        EventStore eventStore = new InMemoryEventStore();
        CommandBus cb = AsyncCommandBus.builder()
                .commandHandlers(new ArrayList<>()).build();
        eventStore.publish(new SomeEvent(aggregateIdentifier.toString()));

        final MyAggregate myAggregate = new MyAggregate(aggregateIdentifier);
        AggregateRepository<MyAggregate> repository = new AbstractRepository<MyAggregate, AnnotatedAggregate<MyAggregate>>(
                new DefaultAggregateModel<>(MyAggregate.class)
        ) {
            @Override
            protected AnnotatedAggregate<MyAggregate> doLoadOrCreate(String aggregateIdentifier, Callable<MyAggregate> factoryMethod) throws Exception {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            protected AnnotatedAggregate<MyAggregate> doCreateNew(Callable<MyAggregate> factoryMethod) throws Exception {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            protected AnnotatedAggregate<MyAggregate> doLoad(String aggregateIdentifier) {
                return new AnnotatedAggregate<>(new DefaultAggregateModel<>(MyAggregate.class), eventStore, myAggregate);
            }

            @Override
            protected AnnotatedAggregate<MyAggregate> doLoad(String aggregateIdentifier, Long expectedVersion) {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            protected void doSave(AnnotatedAggregate<MyAggregate> aggregate) {

            }

            @Override
            protected void doDelete(AnnotatedAggregate<MyAggregate> aggregate) {

            }
        };
        cb.registerHandler(new MyCommandHandler(repository));

//        long COMMAND_COUNT = 5 * 1000 * 1000; // 5 million
        long COMMAND_COUNT = 5 * 1000 * 100; // 5 hundred thousand
//        long COMMAND_COUNT = 5 * 1000; // 5 thousand
        cb.dispatch(new CommandGeneric(aggregateIdentifier.toString(), "ready,"));
        long t1 = System.currentTimeMillis();
        for (int t = 0; t < COMMAND_COUNT; t++) {
            cb.dispatch(new CommandGeneric(aggregateIdentifier.toString(), "go!"));
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("Just did %d commands per second%n", ((COMMAND_COUNT * 1000) / (t2 - t1)));
//        System.out.printf("Just did %d commands per second%n", ((COMMAND_COUNT) / (t2 - t1)));
    }

    public static class MyAggregate {

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        @TargetAggregateIdentifier
        private final UUID identifier;

        @SuppressWarnings("unused")
        protected MyAggregate() {
            this(UUID.randomUUID());
        }

        private MyAggregate(UUID identifier) {
            this.identifier = identifier;
        }

        public void doSomething() {
            apply(new SomeEvent(identifier.toString()));
        }

        @EventSourcingHandler
        public void aa(SomeEvent event) {
            System.out.println("Event processed: " + event);
        }
    }

    public static record CommandGeneric(
            String commandId,
            String commandType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId,
            String text
    ) implements Command {

        public CommandGeneric(String aggregateId, String text) {
            this(UUID.randomUUID().toString(), "CommandGeneric", Instant.now(), aggregateId, 0, "source", "traceId", text);
        }
    }

    public static record SomeEvent(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId
    ) implements DomainEvent {

        public SomeEvent(String aggregateId) {
            this(UUID.randomUUID().toString(), "SomeEvent", Instant.now(), aggregateId, 0, "source", "traceId");
        }
    }

    public static class InMemoryEventStore implements EventStore {

        private final Map<String, List<DomainEvent>> store = new ConcurrentHashMap<>();

        @Override
        public List<DomainEvent> readEvents(String aggregateIdentifier) {
            return store.get(aggregateIdentifier) == null ? new ArrayList<>() : store.get(aggregateIdentifier);
        }

        @Override
        public List<DomainEvent> readFirstEvents() {
            return store.values().stream()
                    .findFirst()
                    .orElse(Collections.emptyList());
        }

        @Override
        public void publish(DomainEvent event) {
            if (CurrentUnitOfWork.isStarted()) {
                CurrentUnitOfWork.get().onPrepareCommit(u -> {
                    System.out.println("stored event: " + event);
                    store.computeIfAbsent(event.aggregateId(), k -> new ArrayList<>()).add(event);
                });
            } else {
                store.computeIfAbsent(event.aggregateId(), k -> new ArrayList<>()).add(event);
            }
        }
    }

    public static class MyCommandHandler implements MessageHandler<Command> {

        private final AggregateRepository<MyAggregate> repository;

        private MyCommandHandler(AggregateRepository<MyAggregate> repository) {
            this.repository = repository;
        }

        @Override
        public Object handle(Command message) throws Exception {
            repository.load(message.aggregateId()).execute(MyAggregate::doSomething);
            return null;
        }
    }
}
