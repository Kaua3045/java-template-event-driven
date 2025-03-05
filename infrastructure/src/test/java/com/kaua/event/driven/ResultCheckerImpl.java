package com.kaua.event.driven;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.uow.DefaultUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.*;

public class ResultCheckerImpl<T> implements ResultChecker<T>, CommandCallBack<Object, Object> {

    private final List<DomainEvent> publishedEvents;
    private final Reporter reporter = new Reporter();
    //    private final FieldFilter fieldFilter;
    private final Supplier<Aggregate<T>> state;
    private ResultMessage<?> actualReturnValue;
    private Throwable actualException;
    public ResultCheckerImpl(List<DomainEvent> publishedEvents,
//                               FieldFilter fieldFilter,
                             Supplier<Aggregate<T>> aggregateState
//                               StubDeadlineManager stubDeadlineManager
    ) {
        this.publishedEvents = publishedEvents;
//        this.fieldFilter = fieldFilter;
        this.state = aggregateState;
//        this.deadlineManagerValidator = new DeadlineManagerValidator(stubDeadlineManager, fieldFilter);
    }

    @Override
    public ResultChecker<T> expectEvents(Object... expectedEvents) {
        if (expectedEvents.length != publishedEvents.size()) {
            reporter.reportWrongEvent(publishedEvents, Arrays.asList(expectedEvents), actualException);
        }

        Iterator<DomainEvent> iterator = publishedEvents.iterator();
        for (Object expectedEvent : expectedEvents) {
            DomainEvent actualEvent = iterator.next();
            if (!verifyPayloadEquality(expectedEvent, actualEvent)) {
                reporter.reportWrongEvent(publishedEvents, Arrays.asList(expectedEvents), actualException);
            }
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectEvents(DomainEvent... expectedEvents) {
        this.expectEvents(Stream.of(expectedEvents).toArray());

        Iterator<DomainEvent> iterator = publishedEvents.iterator();
        for (DomainEvent expectedEvent : expectedEvents) {
            DomainEvent actualEvent = iterator.next();
            // TODO check this
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectEventsMatching(Matcher<? extends List<? super DomainEvent>> matcher) {
        if (!matcher.matches(publishedEvents)) {
            final Description expectation = new StringDescription();
            matcher.describeTo(expectation);

            final Description mismatch = new StringDescription();
            matcher.describeMismatch(publishedEvents, mismatch);

            reporter.reportWrongEvent(publishedEvents, expectation, mismatch, actualException);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectSuccessfulHandlerExecution() {
        return expectResultMessageMatching(anything());
    }

    @Override
    public ResultChecker<T> expectState(Consumer<T> aggregateStateValidator) {
        DefaultUnitOfWork<Command> uow = DefaultUnitOfWork.startAndGet(null);
        try {
            state.get().execute(aggregateStateValidator);
        } finally {
            uow.rollback();
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectResultMessagePayload(Object expectedPayload) {
        StringDescription expectedDescription = new StringDescription();
        StringDescription actualDescription = new StringDescription();
//        PayloadMatcher<CommandResultMessage<?>> expectedMatcher =
//                new PayloadMatcher<>(CoreMatchers.equalTo(expectedPayload));
//        expectedMatcher.describeTo(expectedDescription);
//        if (actualException != null) {
//            reporter.reportUnexpectedException(actualException, expectedDescription);
//        } else if (!verifyPayloadEquality(expectedPayload, actualReturnValue.getPayload())) {
//            PayloadMatcher<CommandResultMessage<?>> actualMatcher =
//                    new PayloadMatcher<>(CoreMatchers.equalTo(actualReturnValue.getPayload()));
//            actualMatcher.describeTo(actualDescription);
//            reporter.reportWrongResult(actualDescription, expectedDescription);
//        }
        return this;
    }

    @Override
    public ResultChecker<T> expectResultMessagePayloadMatching(Matcher<?> matcher) {
        if (matcher == null) {
            return expectResultMessagePayloadMatching(nullValue());
        }
        StringDescription expectedDescription = new StringDescription();
        matcher.describeTo(expectedDescription);
//        if (actualException != null) {
//            reporter.reportUnexpectedException(actualException, expectedDescription);
//        } else if (!matcher.matches(actualReturnValue.getPayload())) {
//            reporter.reportWrongResult(actualReturnValue.getPayload(), expectedDescription);
//        }
        return this;
    }

    @Override
    public ResultChecker<T> expectResultMessage(ResultMessage<?> expectedResultMessage) {
        expectResultMessagePayload(expectedResultMessage.getResult());

        StringDescription expectedDescription = new StringDescription();
        StringDescription actualDescription = new StringDescription();
        // TODO check this
//        MapEntryMatcher expectedMatcher = new MapEntryMatcher(expectedResultMessage.getMetaData());
//        MapEntryMatcher actualMatcher = new MapEntryMatcher(actualReturnValue.getMetaData());
//        expectedMatcher.describeTo(expectedDescription);
//        actualMatcher.describeTo(actualDescription);
//        if (!verifyMetaDataEquality(expectedResultMessage.getPayloadType(),
//                expectedResultMessage.getMetaData(),
//                actualReturnValue.getMetaData())) {
//            reporter.reportWrongResult(actualDescription, expectedDescription);
//        }

        return this;
    }

    @Override
    public ResultChecker<T> expectResultMessageMatching(Matcher<? super ResultMessage<?>> matcher) {
        if (matcher == null) {
            return expectResultMessageMatching(nullValue());
        }
        StringDescription expectedDescription = new StringDescription();
        matcher.describeTo(expectedDescription);
        if (actualException != null) {
            reporter.reportUnexpectedException(actualException, expectedDescription);
        } else if (!matcher.matches(actualReturnValue)) {
            reporter.reportWrongResult(actualReturnValue, expectedDescription);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectExceptionMessage(Matcher<?> exceptionMessageMatcher) {
        StringDescription emptyMatcherDescription = new StringDescription(
                new StringBuilder("Given exception message matcher is null!"));
        if (exceptionMessageMatcher == null) {
            reporter.reportWrongExceptionMessage(actualException, emptyMatcherDescription);
            return this;
        }

        StringDescription description = new StringDescription();
        exceptionMessageMatcher.describeTo(description);

        if (actualException == null) {
            reporter.reportUnexpectedReturnValue(actualReturnValue.getResult(), description);
        }
        if (actualException != null && !exceptionMessageMatcher.matches(actualException.getMessage())) {
            reporter.reportWrongExceptionMessage(actualException, description);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectExceptionMessage(String exceptionMessage) {
        return expectExceptionMessage(CoreMatchers.equalTo(exceptionMessage));
    }

    @Override
    public ResultChecker<T> expectException(Class<? extends Throwable> expectedException) {
        return expectException(instanceOf(expectedException));
    }

    @Override
    public ResultChecker<T> expectException(Matcher<?> matcher) {
        StringDescription description = new StringDescription();
        matcher.describeTo(description);
        if (actualException == null) {
            reporter.reportUnexpectedReturnValue(actualReturnValue.getResult(), description);
        }
        if (!matcher.matches(actualException)) {
            reporter.reportWrongException(actualException, description);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Object exceptionDetails) {
        return expectExceptionDetails(CoreMatchers.equalTo(exceptionDetails));
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Class<?> exceptionDetails) {
        return expectExceptionDetails(instanceOf(exceptionDetails));
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Matcher<?> exceptionDetailsMatcher) {
        Object actualDetails = actualException.getMessage();
        if (exceptionDetailsMatcher == null) {
            StringDescription emptyMatcherDescription = new StringDescription(
                    new StringBuilder("Given exception details matcher is null!"));
            reporter.reportWrongExceptionDetails(actualDetails, emptyMatcherDescription);
            return this;
        }
        if (actualException == null) {
            StringDescription description = new StringDescription(new StringBuilder(
                    "an exception with details matching "));
            exceptionDetailsMatcher.describeTo(description);
            reporter.reportUnexpectedReturnValue(actualReturnValue.getResult(), description);
            return this;
        }
        if (!exceptionDetailsMatcher.matches(actualDetails)) {
            StringDescription description = new StringDescription();
            exceptionDetailsMatcher.describeTo(description);
            reporter.reportWrongExceptionDetails(actualDetails, description);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectMarkedDeleted() {
        if (!state.get().isDeleted()) {
            reporter.reportIncorrectDeletedState(true);
        }

        return this;
    }

    @Override
    public ResultChecker<T> expectNotMarkedDeleted() {
        if (state.get().isDeleted()) {
            reporter.reportIncorrectDeletedState(false);
        }

        return this;
    }

    @Override
    public void onResult(Object command, ResultMessage<?> result) {
        if (result.isExceptional()) {
            recordException(result.getExceptionResult());
        } else {
            actualReturnValue = result;
        }
    }

    public void recordException(Throwable exception) {
        this.actualException = exception;
    }

    public void assertValidRecording() {
        if (actualException instanceof Error) {
            throw (Error) actualException;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean verifyPayloadEquality(Object expectedPayload, Object actualPayload) {
        if (Objects.equals(expectedPayload, actualPayload)) {
            return true;
        }
        if (expectedPayload != null && actualPayload == null) {
            return false;
        }
        if (expectedPayload == null) {
            return false;
        }
        if (!expectedPayload.getClass().equals(actualPayload.getClass())) {
            return false;
        }

        // TODO check this
//        Matcher<Object> matcher = deepEquals(expectedPayload, fieldFilter);
//        if (!matcher.matches(actualPayload)) {
//            reporter.reportDifferentPayloads(expectedPayload.getClass(), actualPayload, expectedPayload);
//        }
        return true;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean verifyMetaDataEquality(Class<?> eventType, Map<String, Object> expectedMetaData,
                                           Map<String, Object> actualMetaData) {
        MapEntryMatcher matcher = new MapEntryMatcher(expectedMetaData);
        if (!matcher.matches(actualMetaData)) {
            reporter.reportDifferentMetaData(eventType, matcher.getMissingEntries(), matcher.getAdditionalEntries());
        }
        return true;
    }
}
