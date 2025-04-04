package com.kaua.event.driven.fixture;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public interface TestExecutor<T> {

    ResultChecker<T> when(Object command);

    ResultChecker<T> when(Object command, Map<String, ?> metaData);

    TestExecutor<T> andGiven(Object... domainEvents);

    TestExecutor<T> andGiven(List<?> domainEvents);

    TestExecutor<T> andGivenCommands(Object... commands);

    TestExecutor<T> andGivenCommands(List<?> commands);

    TestExecutor<T> andGivenCurrentTime(Instant currentTime);

    Instant currentTime();

    @Deprecated
    default ResultChecker andThenTimeElapses(Duration elapsedTime) {
        return whenTimeElapses(elapsedTime);
    }

    @Deprecated
    ResultChecker<T> whenThenTimeElapses(Duration elapsedTime);

    default ResultChecker<T> whenTimeElapses(Duration elapsedTime) {
        return whenThenTimeElapses(elapsedTime);
    }

    @Deprecated
    default ResultChecker andThenTimeAdvancesTo(Instant newPointInTime) {
        return whenTimeAdvancesTo(newPointInTime);
    }

    @Deprecated
    ResultChecker<T> whenThenTimeAdvancesTo(Instant newPointInTime);

    default ResultChecker<T> whenTimeAdvancesTo(Instant newPointInTime) {
        return whenThenTimeAdvancesTo(newPointInTime);
    }

    ResultChecker<T> whenConstructing(Callable<T> aggregateFactory);

    ResultChecker<T> whenInvoking(String aggregateIdentifier, Consumer<T> aggregateConsumer);
}
