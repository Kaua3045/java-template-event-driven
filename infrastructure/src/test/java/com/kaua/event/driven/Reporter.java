package com.kaua.event.driven;

import com.kaua.event.driven.domain.events.DomainEvent;
import org.hamcrest.Description;
import org.hamcrest.StringDescription;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Reporter {

    private static final String NEWLINE = System.getProperty("line.separator");

    public void reportWrongEvent(Collection<?> actualEvents, Collection<?> expectedEvents,
                                 Throwable probableCause) {
        StringBuilder sb = new StringBuilder(
                "The published events do not match the expected events");
        appendEventOverview(sb, expectedEvents, actualEvents);
        appendProbableCause(probableCause, sb);

        throw new RuntimeException(sb.toString());
    }

    @Deprecated
    public void reportWrongEvent(Collection<?> actualEvents, StringDescription expectation, Throwable probableCause) {
        StringBuilder sb = new StringBuilder(
                "The published events do not match the expected events.");
        sb.append("Expected:")
                .append(NEWLINE)
                .append(expectation)
                .append(NEWLINE)
                .append(" But got");
        if (actualEvents.isEmpty()) {
            sb.append(" none");
        } else {
            sb.append(":");
        }
        for (Object publishedEvent : actualEvents) {
            sb.append(NEWLINE)
                    .append(publishedEvent.getClass().getSimpleName())
                    .append(": ")
                    .append(publishedEvent);
        }
        appendProbableCause(probableCause, sb);

        throw new RuntimeException(sb.toString());
    }

    public void reportWrongEvent(Collection<?> actualEvents, Description expectation, Description mismatch, Throwable probableCause) {
        StringBuilder sb = new StringBuilder("The published events do not match the expected events.");
        sb.append("Expected <")
                .append(expectation)
                .append(">,")
                .append(NEWLINE)
                .append(" but got <")
                .append(mismatch)
                .append(">.")
                .append(NEWLINE);
        sb.append("Actual Sequence of events:");
        if (actualEvents.isEmpty()) {
            sb.append(" no events emitted");
        }
        for (Object publishedEvent : actualEvents) {
            sb.append(NEWLINE);
            sb.append(publishedEvent.getClass().getSimpleName());
            sb.append(": ");
            sb.append(publishedEvent);
        }
        appendProbableCause(probableCause, sb);

        throw new RuntimeException(sb.toString());

    }

    public void reportUnexpectedException(Throwable actualException, Description expectation) {
        StringBuilder sb = new StringBuilder("The command handler threw an unexpected exception");
        sb.append(NEWLINE)
                .append(NEWLINE)
                .append("Expected <")
                .append(expectation.toString())
                .append(">,")
                .append(NEWLINE)
                .append(" but got <exception of type [")
                .append(actualException.getClass().getSimpleName())
                .append("]>.")
                .append(NEWLINE)
                .append("Stack trace follows:")
                .append(NEWLINE);
        writeStackTrace(actualException, sb);
        sb.append(NEWLINE);
        throw new RuntimeException(sb.toString());
    }

    public void reportWrongResult(Object actual, Object expectation) {
        StringBuilder sb = new StringBuilder("The command handler returned an unexpected value");
        sb.append(NEWLINE)
                .append(NEWLINE)
                .append("Expected <")
                .append(expectation.toString())
                .append(">,")
                .append(NEWLINE)
                .append(" but got <");
        describe(actual, sb);
        sb.append(">.")
                .append(NEWLINE);
        throw new RuntimeException(sb.toString());
    }

    public void reportUnexpectedReturnValue(Object actualReturnValue, Description description) {
        StringBuilder sb = new StringBuilder("The command handler returned normally, but an exception was expected");
        sb.append(NEWLINE)
                .append(NEWLINE)
                .append("Expected <")
                .append(description.toString())
                .append(">,")
                .append(NEWLINE)
                .append(" but got <");
        describe(actualReturnValue, sb);
        sb.append(">.")
                .append(NEWLINE);
        throw new RuntimeException(sb.toString());
    }

    public void reportWrongException(Throwable actualException, Description description) {
        StringBuilder sb = new StringBuilder("The command handler threw an exception, but not of the expected type")
                .append(NEWLINE)
                .append(NEWLINE)
                .append("Expected <")
                .append(description.toString())
                .append(">,")
                .append(NEWLINE)
                .append(" but got <exception of type [")
                .append(actualException.getClass().getSimpleName())
                .append("]>.")
                .append(NEWLINE)
                .append("Stack trace follows:")
                .append(NEWLINE);
        writeStackTrace(actualException, sb);
        sb.append(NEWLINE);
        throw new RuntimeException(sb.toString());
    }

    public void reportWrongExceptionMessage(Throwable actualException, Description description) {
        throw new RuntimeException("The command handler threw an exception, but not with expected message"
                + NEWLINE
                + NEWLINE
                + "Expected <"
                + description.toString()
                + ">, "
                + NEWLINE
                + " but got <message ["
                + actualException.getMessage()
                + "]>."
                + NEWLINE
                + NEWLINE);
    }

    public void reportWrongExceptionDetails(Object details, Description description) {
        throw new RuntimeException("The command handler threw an exception, but not with expected details"
                + NEWLINE
                + NEWLINE
                + "Expected <"
                + description.toString()
                + ">,"
                + NEWLINE
                + " but got <details ["
                + details
                + "]>."
                + NEWLINE
                + NEWLINE);
    }

    public void reportDifferentPayloads(Class<?> messageType, Object actual, Object expected) {
        throw new RuntimeException("One of the messages contained a different payload than expected"
                + NEWLINE
                + NEWLINE
                + "The message of type [" + messageType.getSimpleName() + "] "
                + "was not as expected."
                + NEWLINE
                + "Expected <"
                + nullSafeToString(expected)
                + ">,"
                + NEWLINE
                + " but got <"
                + nullSafeToString(actual)
                + ">."
                + NEWLINE
                + NEWLINE);
    }

    public void reportDifferentPayloads(Class<?> messageType, Field field, Object actual, Object expected) {
        StringBuilder sb = new StringBuilder("One of the messages contained different values than expected");
        sb.append(NEWLINE)
                .append(NEWLINE)
                .append("In a message of type [")
                .append(messageType.getSimpleName())
                .append("], the property [")
                .append(field.getName())
                .append("] ");
        if (!messageType.equals(field.getDeclaringClass())) {
            sb.append("(declared in [")
                    .append(field.getDeclaringClass().getSimpleName())
                    .append("]) ");
        }

        sb.append("was not as expected.")
                .append(NEWLINE)
                .append("Expected <")
                .append(nullSafeToString(expected))
                .append(">,")
                .append(NEWLINE)
                .append(" but got <")
                .append(nullSafeToString(actual))
                .append(">.")
                .append(NEWLINE);
        throw new RuntimeException(sb.toString());
    }

    public void reportDifferentMetaData(Class<?> messageType, Map<String, Object> missingEntries,
                                        Map<String, Object> additionalEntries) {
        StringBuilder sb = new StringBuilder("One of the messages contained different metadata than expected");
        sb.append(NEWLINE)
                .append(NEWLINE)
                .append("In a message of type [")
                .append(messageType.getSimpleName())
                .append("], ");
        if (!additionalEntries.isEmpty()) {
            sb.append("metadata entries")
                    .append(NEWLINE)
                    .append("[");
            for (Map.Entry<String, Object> entry : additionalEntries.entrySet()) {
                sb.append(entryAsString(entry))
                        .append(", ");
            }
            sb.delete(sb.lastIndexOf(", "), sb.lastIndexOf(",") + 2);
            sb.append("] ")
                    .append(NEWLINE)
                    .append("were not expected. ");
        }
        if (!missingEntries.isEmpty()) {
            sb.append("metadata entries ")
                    .append(NEWLINE)
                    .append("[");
            for (Map.Entry<String, Object> entry : missingEntries.entrySet()) {
                sb.append(entryAsString(entry))
                        .append(", ");
            }
            sb.delete(sb.lastIndexOf(","), sb.lastIndexOf(",") + 2);
            sb.append("] ")
                    .append(NEWLINE)
                    .append("were expected but not seen.");
        }
        throw new RuntimeException(sb.toString());
    }

    public void reportIncorrectDeletedState(boolean expectedDeletedState) {
        if (expectedDeletedState) {
            throw new RuntimeException("Aggregate should have been marked for deletion, but this did not happen");
        } else {
            throw new RuntimeException("Aggregate has been marked for deletion, but this was not expected");
        }
    }

    private void appendProbableCause(Throwable probableCause, StringBuilder sb) {
        if (probableCause != null) {
            sb.append(NEWLINE);
            sb.append("A probable cause for the wrong chain of events is an "
                    + "exception that occurred while handling the command.");
            sb.append(NEWLINE);
            CharArrayWriter charArrayWriter = new CharArrayWriter();
            probableCause.printStackTrace(new PrintWriter(charArrayWriter));
            sb.append(charArrayWriter.toCharArray());
        }
    }

    private void writeStackTrace(Throwable actualException, StringBuilder sb) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(baos);
        actualException.printStackTrace(pw);
        pw.flush();
        sb.append(new String(baos.toByteArray()));
    }

    private String nullSafeToString(final Object value) {
        if (value == null) {
            return "<null>";
        }
        return value.toString();
    }

    private void describe(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else {
            sb.append(value);
        }
    }

    private String entryAsString(Map.Entry<?, ?> entry) {
        if (entry == null) {
            return "<null>=<null>";
        } else {
            return nullSafeToString(entry.getKey()) + "=" + nullSafeToString(entry.getValue());
        }
    }

    private void appendEventOverview(StringBuilder sb, Collection<?> leftColumnEvents,
                                     Collection<?> rightColumnEvents) {
        List<String> actualTypes = new ArrayList<>(rightColumnEvents.size());
        List<String> expectedTypes = new ArrayList<>(leftColumnEvents.size());
        int largestExpectedSize = 8;
        actualTypes.addAll(rightColumnEvents.stream().map((Function<Object, String>) this::payloadContentType)
                .collect(Collectors.toList()));
        for (Object event : leftColumnEvents) {
            String simpleName = payloadContentType(event);
            if (simpleName.length() > largestExpectedSize) {
                largestExpectedSize = simpleName.length();
            }
            expectedTypes.add(simpleName);
        }
        sb.append(NEWLINE);
        sb.append(NEWLINE);
        sb.append("Expected");
        pad(sb, "Expected".length(), largestExpectedSize, " ");
        sb.append("  |  ")
                .append("Actual")
                .append(NEWLINE);
        pad(sb, 0, largestExpectedSize, "-");
        sb.append("--|--");
        pad(sb, 0, largestExpectedSize, "-");
        sb.append(NEWLINE);
        Iterator<String> actualIterator = actualTypes.iterator();
        Iterator<String> expectedIterator = expectedTypes.iterator();
        while (actualIterator.hasNext() || expectedIterator.hasNext()) {
            boolean difference;
            String expected = "";
            if (expectedIterator.hasNext()) {
                expected = expectedIterator.next();
                sb.append(expected);
                pad(sb, expected.length(), largestExpectedSize, " ");
                difference = !actualIterator.hasNext();
            } else {
                pad(sb, 0, largestExpectedSize, " ");
                difference = true;
            }
            if (actualIterator.hasNext()) {
                String actual = actualIterator.next();
                difference = difference || !actual.equals(expected);
                if (difference) {
                    sb.append(" <|> ");
                } else {
                    sb.append("  |  ");
                }
                sb.append(actual);
            } else {
                sb.append(" <|> ");
            }

            sb.append(NEWLINE);
        }
    }

    private String payloadContentType(Object event) {
        String simpleName;
        if (event instanceof DomainEvent) {
            simpleName = ((DomainEvent) event).getClass().getName();
        } else {
            simpleName = event.getClass().getName();
        }
        return simpleName;
    }

    private void pad(StringBuilder sb, int currentLength, int targetLength, String character) {
        for (int t = currentLength; t < targetLength; t++) {
            sb.append(character);
        }
    }
}
