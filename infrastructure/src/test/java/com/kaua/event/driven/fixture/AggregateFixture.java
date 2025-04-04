package com.kaua.event.driven.fixture;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateCommandHandlerAnnotated;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateRepository;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.EventSourcingRepository;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.es.command.impl.SimpleCommandBus;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageDispatcherInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.lock.NullLockFactory;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AggregateFixture<T> implements FixtureTestConfiguration<T>, TestExecutor<T> {

    private static final Logger log = LoggerFactory.getLogger(AggregateFixture.class);

    private final Class<T> aggregateType;
    private final SimpleCommandBus commandBus;
    private final EventStore eventStore;
    private Deque<DomainEvent> givenEvents;
    private Deque<DomainEvent> storedEvents;
    private List<DomainEvent> publishedEvents;

    public AggregateFixture(Class<T> aggregateType) {
        this.aggregateType = aggregateType;
        this.eventStore = new InMemoryEventStore();
        this.givenEvents = new LinkedList<>();
        this.storedEvents = new LinkedList<>();
        this.publishedEvents = new ArrayList<>();

        Map<Class<?>, AggregateModel<?>> aggregateModels = new ConcurrentHashMap<>();
        aggregateModels.put(aggregateType, new DefaultAggregateModel<>(aggregateType));

        Map<Class<?>, AggregateCommandHandlerAnnotated<?>> commandHandlerMap = new ConcurrentHashMap<>();
        commandHandlerMap.put(aggregateType, AggregateCommandHandlerAnnotated.builder()
                        .aggregateModel(aggregateModels.get(aggregateType))
                        .repository((AggregateRepository<Object>) new EventSourcingRepository<>(eventStore, NullLockFactory.INSTANCE, aggregateModels.get(aggregateType)))
                .build());


        this.commandBus = SimpleCommandBus.builder()
                .commandHandlers(new ArrayList<>())
                .build();

        commandHandlerMap.forEach((key, value) -> commandBus.registerHandler(value));
    }

    @Override
    public FixtureTestConfiguration<T> useStateStorage() {
        return null;
    }

    @Override
    public FixtureTestConfiguration<T> registerRepository(AggregateRepository<T> repository) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FixtureTestConfiguration<T> registerAnnotatedCommandHandler(Object annotatedCommandHandler) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FixtureTestConfiguration<T> registerCommandHandler(Class<?> payloadType, MessageHandler<Command> commandHandler) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FixtureTestConfiguration<T> registerCommandHandler(String commandName, MessageHandler<Command> commandHandler) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FixtureTestConfiguration<T> registerCommandDispatchInterceptor(MessageDispatcherInterceptor<? super Command> commandDispatchInterceptor) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FixtureTestConfiguration<T> registerCommandHandlerInterceptor(MessageHandlerInterceptor<? super Command> commandHandlerInterceptor) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TestExecutor<T> given(Object... domainEvents) {
        return given(Arrays.asList(domainEvents));
    }

    @Override
    public TestExecutor<T> givenState(Supplier<T> aggregateState) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TestExecutor<T> givenNoPriorActivity() {
//        ensureRepositoryConfiguration();
        clearGivenWhenState();
        return this;
    }

    @Override
    public TestExecutor<T> given(List<?> domainEvents) {
//        ensureRepositoryConfiguration();
        clearGivenWhenState();
        return andGiven(domainEvents);
    }

    @Override
    public TestExecutor<T> givenCommands(Object... commands) {
        return givenCommands(Arrays.asList(commands));
    }

    @Override
    public TestExecutor<T> givenCommands(List<?> commands) {
        clearGivenWhenState();
        return andGivenCommands(commands);
    }

    @Override
    public CommandBus getCommandBus() {
        return commandBus;
    }

    @Override
    public EventBus getEventBus() {
        return eventStore;
    }

    @Override
    public EventStore getEventStore() {
        return eventStore;
    }

    @Override
    public AggregateRepository<T> getRepository() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TestExecutor<T> givenCurrentTime(Instant time) {
        return null;
    }

    @Override
    public void setReportIllegalStateChange(boolean reportIllegalStateChange) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> when(Object command) {
        return when(command, Collections.emptyMap());
    }

    @Override
    public ResultChecker<T> when(Object command, Map<String, ?> metaData) {
        return when(resultChecker -> commandBus.dispatch((Command) command, resultChecker));
    }

    @Override
    public TestExecutor<T> andGiven(Object... domainEvents) {
        return andGiven(Arrays.asList(domainEvents));
    }

    @Override
    public TestExecutor<T> andGiven(List<?> domainEvents) {
        for (Object event : domainEvents) {
            this.givenEvents.add((DomainEvent) event);
        }
        return this;
    }

    @Override
    public TestExecutor<T> andGivenCommands(Object... commands) {
        return andGivenCommands(Arrays.asList(commands));
    }

    @Override
    public TestExecutor<T> andGivenCommands(List<?> commands) {
        for (Object command : commands) {
            ExecutionExceptionAwareCallback callback = new ExecutionExceptionAwareCallback();
            commandBus.dispatch((Command) command, callback);
            callback.assertSuccessful();
            givenEvents.addAll(storedEvents);
            storedEvents.clear();
        }
        publishedEvents.clear();
        return this;
    }

    @Override
    public TestExecutor<T> andGivenCurrentTime(Instant currentTime) {
        return null;
    }

    @Override
    public Instant currentTime() {
        return null;
    }

    @Override
    public ResultChecker<T> whenThenTimeElapses(Duration elapsedTime) {
        return null;
    }

    @Override
    public ResultChecker<T> whenThenTimeAdvancesTo(Instant newPointInTime) {
        return null;
    }

    @Override
    public ResultChecker<T> whenConstructing(Callable<T> aggregateFactory) {
        return null;
    }

    @Override
    public ResultChecker<T> whenInvoking(String aggregateIdentifier, Consumer<T> aggregateConsumer) {
        return null;
    }

    private ResultChecker<T> when(Consumer<ResultCheckerImpl<T>> whenPhase) {
        log.info("when phase started");
        ResultCheckerImpl<T> resultChecker = new ResultCheckerImpl<>(publishedEvents);

        whenPhase.accept(resultChecker);

        log.info("when phase end and starting expect phase");
        return resultChecker;
    }

    private void clearGivenWhenState() {
        log.info("clearing given state");
        storedEvents = new LinkedList<>();
        publishedEvents = new ArrayList<>();
        givenEvents = new LinkedList<>();
    }

    private static class ExecutionExceptionAwareCallback implements CommandCallBack<Object, Object> {

        private InternalErrorException exception;

        @Override
        public void onResult(Object command, ResultMessage<?> result) {
            if (result.isExceptional()) {
                exception = (InternalErrorException) result.getExceptionResult();
            }
        }

        public void assertSuccessful() {
            if (exception != null) {
                throw exception;
            }
        }
    }

    private class InMemoryEventStore implements EventStore {

        private final Map<String, List<DomainEvent>> store = new ConcurrentHashMap<>();

        @Override
        public List<DomainEvent> readEvents(String aggregateIdentifier) {
            List<DomainEvent> allEvents = new ArrayList<>(givenEvents);
            allEvents.addAll(storedEvents);

            if (allEvents.isEmpty()) {
                return Collections.emptyList();
            }

            return allEvents;
        }

        @Override
        public List<DomainEvent> readFirstEvents() {
            List<DomainEvent> allEvents = new ArrayList<>(givenEvents);
            allEvents.addAll(storedEvents);

            if (allEvents.isEmpty()) {
                return Collections.emptyList();
            }

            return allEvents;
        }

        @Override
        public void publish(DomainEvent event) {
            if (CurrentUnitOfWork.isStarted()) {
                CurrentUnitOfWork.get().onPrepareCommit(u -> {
                    System.out.println("stored event: " + event);
                    store.computeIfAbsent(event.aggregateId(), k -> new ArrayList<>()).add(event);
                    publishedEvents.add(event);
                });
            } else {
                store.computeIfAbsent(event.aggregateId(), k -> new ArrayList<>()).add(event);
                publishedEvents.add(event);
                System.out.println("published event: " + publishedEvents.size());
            }
        }
    }
}
