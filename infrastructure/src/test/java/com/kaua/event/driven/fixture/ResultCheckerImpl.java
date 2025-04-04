package com.kaua.event.driven.fixture;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.command.callback.CommandCallBack;
import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.nullValue;

public class ResultCheckerImpl<T> implements ResultChecker<T>, CommandCallBack<Object, Object> {

    private final List<DomainEvent> publishedEvents;
    private ResultMessage<?> actualReturnValue;
    private Throwable actualException;

    public ResultCheckerImpl(List<DomainEvent> publishedEvents) {
        this.publishedEvents = publishedEvents;
    }

    @Override
    public ResultChecker<T> expectEvents(Object... expectedEvents) {
        if (expectedEvents.length != publishedEvents.size()) {
            throw new AssertionError("Número de eventos publicados não corresponde ao esperado.");
        }

        Iterator<DomainEvent> iterator = publishedEvents.iterator();
        for (Object expectedEvent : expectedEvents) {
            DomainEvent actualEvent = iterator.next();
            if (!verifyPayloadEquality((DomainEvent) expectedEvent, actualEvent)) {
                throw new AssertionError("Evento publicado não corresponde ao esperado.");
            }
        }

        return this;
    }

    @Override
    public ResultChecker<T> expectEvents(DomainEvent... expectedEvents) {
        System.out.println(expectedEvents.length);
        System.out.println(publishedEvents.size());
        if (expectedEvents.length != publishedEvents.size()) {
            throw new AssertionError("Número de eventos publicados não corresponde ao esperado.");
        }

        Iterator<DomainEvent> iterator = publishedEvents.iterator();
        for (DomainEvent expectedEvent : expectedEvents) {
            DomainEvent actualEvent = iterator.next();
            if (!verifyPayloadEquality(expectedEvent, actualEvent)) {
                throw new AssertionError("Evento publicado não corresponde ao esperado.");
            }
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

            throw new AssertionError("Published events did not match expectation. Expected: " + expectation
                    + ", but got: " + mismatch);
        }

        return this;
    }

    @Override
    public ResultChecker<T> expectResultMessagePayload(Object expectedPayload) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectResultMessagePayloadMatching(Matcher<?> matcher) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectResultMessage(ResultMessage<?> expectedResultMessage) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectResultMessageMatching(Matcher<? super ResultMessage<?>> matcher) {
        if (matcher == null) {
            return expectResultMessageMatching(nullValue());
        }

        StringDescription stringDescription = new StringDescription();
        matcher.describeTo(stringDescription);
        if (actualException != null) {
            throw new AssertionError("Handler execution resulted in an exception, while a successful result was expected. "
                    + "Exception: " + actualException.getMessage() + ". Matcher: " + stringDescription);
        } else if (!matcher.matches(actualReturnValue)) {
            throw new AssertionError("Handler execution returned an unexpected result. Expected: " + stringDescription
                    + ", but got: " + actualReturnValue);
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectExceptionMessage(Matcher<?> matcher) {
        StringDescription emptyDescription = new StringDescription(
                new StringBuilder("Given matcher is null. Expected exception message should not be null."));
        if (matcher == null) {
            throw new AssertionError(emptyDescription.toString());
        }

        StringDescription stringDescription = new StringDescription();
        matcher.describeTo(stringDescription);

        if (actualException == null) {
            throw new AssertionError("Handler execution did not result in an exception, while an exception was expected. "
                    + "Matcher: " + stringDescription);
        } else if (!matcher.matches(actualException.getMessage())) {
            throw new AssertionError("Handler execution resulted in an exception with an unexpected message. Expected: "
                    + stringDescription + ", but got: " + actualException.getMessage());
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
            throw new AssertionError("Handler execution did not result in an exception, while an exception was expected. "
                    + "Matcher: " + description);
        } else if (!matcher.matches(actualException)) {
            throw new AssertionError("Handler execution resulted in an exception of an unexpected type. Expected: "
                    + description + ", but got: " + actualException.getClass().getName());
        }
        return this;
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Object exceptionDetails) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Class<?> exceptionDetails) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectExceptionDetails(Matcher<?> matcher) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectSuccessfulHandlerExecution() {
        return expectResultMessageMatching(anything());
    }

    @Override
    public ResultChecker<T> expectState(Consumer<T> aggregateStateValidator) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ResultChecker<T> expectMarkedDeleted() {
        throw new UnsupportedOperationException("need state implementation");
    }

    @Override
    public ResultChecker<T> expectNotMarkedDeleted() {
        throw new UnsupportedOperationException("need state implementation");
    }

    @Override
    public void onResult(Object command, ResultMessage<?> result) {
        if (result.isExceptional()) {
            recordException(result.getExceptionResult());
        } else {
            actualReturnValue = result;
        }
    }

    public void recordException(Throwable ex) {
        this.actualException = ex;
    }

    private boolean verifyPayloadEquality(DomainEvent expectedEvent, DomainEvent actualEvent) {
        // Obtenha todos os campos da classe DomainEvent
        if (!expectedEvent.getClass().isAssignableFrom(actualEvent.getClass())) {
            return false;
        }

        Field[] fields = DomainEvent.class.getDeclaredFields();

        for (Field field : fields) {
            // Ignore os campos eventId e occurredOn
            if (field.getName().equals("eventId") || field.getName().equals("occurredOn")) {
                continue;
            }

            field.setAccessible(true); // Torna o campo acessível, mesmo que seja privado

            try {
                Object expectedValue = field.get(expectedEvent);
                Object actualValue = field.get(actualEvent);

                // Compare os valores dos campos
                if (expectedValue == null ? actualValue != null : !expectedValue.equals(actualValue)) {
                    return false; // Se os valores forem diferentes, retorna false
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                return false; // Se ocorrer algum erro ao acessar os campos, retorna false
            }
        }

        return true; // Se todos os campos forem iguais (exceto eventId e occurredOn), retorna true
    }
}
