package com.kaua.event.driven.fixture;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Consumer;

public interface ResultChecker<T> {

    ResultChecker<T> expectEvents(Object... expectedEvents);

    ResultChecker<T> expectEvents(DomainEvent... expectedEvents);

    default ResultChecker<T> expectNoEvents() {
        return expectEvents();
    }

    ResultChecker<T> expectEventsMatching(Matcher<? extends List<? super DomainEvent>> matcher);

    ResultChecker<T> expectResultMessagePayload(Object expectedPayload);

    ResultChecker<T> expectResultMessagePayloadMatching(Matcher<?> matcher);

    ResultChecker<T> expectResultMessage(ResultMessage<?> expectedResultMessage);

    ResultChecker<T> expectResultMessageMatching(Matcher<? super ResultMessage<?>> matcher);

    ResultChecker<T> expectExceptionMessage(Matcher<?> matcher);

    ResultChecker<T> expectExceptionMessage(String exceptionMessage);

    ResultChecker<T> expectException(Class<? extends Throwable> expectedException);

    ResultChecker<T> expectException(Matcher<?> matcher);

    ResultChecker<T> expectExceptionDetails(Object exceptionDetails);

    ResultChecker<T> expectExceptionDetails(Class<?> exceptionDetails);

    ResultChecker<T> expectExceptionDetails(Matcher<?> matcher);

    ResultChecker<T> expectSuccessfulHandlerExecution();

    ResultChecker<T> expectState(Consumer<T> aggregateStateValidator);

    ResultChecker<T> expectMarkedDeleted();

    ResultChecker<T> expectNotMarkedDeleted();
}
