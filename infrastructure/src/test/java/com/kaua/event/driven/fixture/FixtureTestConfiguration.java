package com.kaua.event.driven.fixture;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateRepository;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.eventbus.EventBus;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageDispatcherInterceptor;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.message.MessageHandler;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

public interface FixtureTestConfiguration<T> {

    FixtureTestConfiguration<T> useStateStorage();

    FixtureTestConfiguration<T> registerRepository(AggregateRepository<T> repository);

    FixtureTestConfiguration<T> registerAnnotatedCommandHandler(Object annotatedCommandHandler);

    FixtureTestConfiguration<T> registerCommandHandler(Class<?> payloadType, MessageHandler<Command> commandHandler);

    FixtureTestConfiguration<T> registerCommandHandler(String commandName, MessageHandler<Command> commandHandler);

    FixtureTestConfiguration<T> registerCommandDispatchInterceptor(
            MessageDispatcherInterceptor<? super Command> commandDispatchInterceptor
    );

    FixtureTestConfiguration<T> registerCommandHandlerInterceptor(
            MessageHandlerInterceptor<? super Command> commandHandlerInterceptor
    );

    TestExecutor<T> given(Object... domainEvents);

    TestExecutor<T> givenState(Supplier<T> aggregateState);

    TestExecutor<T> givenNoPriorActivity();

    TestExecutor<T> given(List<?> domainEvents);

    TestExecutor<T> givenCommands(Object... commands);

    TestExecutor<T> givenCommands(List<?> commands);

    CommandBus getCommandBus();

    EventBus getEventBus();

    EventStore getEventStore();

    AggregateRepository<T> getRepository();

    TestExecutor<T> givenCurrentTime(Instant time);

    void setReportIllegalStateChange(boolean reportIllegalStateChange);
}
