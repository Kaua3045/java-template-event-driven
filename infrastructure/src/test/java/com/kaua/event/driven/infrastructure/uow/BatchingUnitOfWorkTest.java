package com.kaua.event.driven.infrastructure.uow;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kaua.event.driven.infrastructure.uow.UnitOfWork.Phase.*;
import static org.junit.jupiter.api.Assertions.*;

class BatchingUnitOfWorkTest extends UnitTest {

    private List<PhaseTransition> transitions;
    private BatchingUnitOfWork<DomainEvent> subject;

    @BeforeEach
    void setUp() {
        transitions = new ArrayList<>();
    }

    @Test
    void givenADomainEvents_whenCallExecuteWithResult_thenReturnSuccessMessages() {
        List<DomainEvent> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        subject = new BatchingUnitOfWork<>(messages);
        subject.executeWithResult(() -> {
            registerListeners(subject);
            return resultFor(subject.getMessage());
        });
        validatePhaseTransitions(Arrays.asList(PREPARE_COMMIT, COMMIT, AFTER_COMMIT, CLEANUP), messages);
        Map<DomainEvent, ResultMessage<DomainEvent>> expectedResults = new HashMap<>();
        messages.forEach(m -> expectedResults.put(m, ResultMessage.success(m)));
        assertExecutionResults(expectedResults, subject.getExecutionResults());
    }

    @Test
    void givenADomainEvents_whenCallExecuteWithResultButIdOneThrows_thenReturnFailureMessages() {
        List<DomainEvent> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        subject = new BatchingUnitOfWork<>(messages);
        RuntimeException e = new RuntimeException();
        try {
            subject.executeWithResult(() -> {
                registerListeners(subject);
                if (subject.getMessage().aggregateId().equals("1")) {
                    throw e;
                }
                return resultFor(subject.getMessage());
            });
        } catch (Exception ignored) {
        }
        validatePhaseTransitions(Arrays.asList(ROLLBACK, CLEANUP), messages.subList(0, 2));
        Map<DomainEvent, ResultMessage<DomainEvent>> expectedResult = new HashMap<>();
        messages.forEach(m -> expectedResult.put(m, ResultMessage.failure(e)));
        assertExecutionResults(expectedResult, subject.getExecutionResults());
    }

    @Test
    void suppressedExceptionOnRollback() {
        List<DomainEvent> messages = Arrays.asList(toMessage(0), toMessage(1), toMessage(2));
        AtomicInteger cleanupCounter = new AtomicInteger();
        subject = new BatchingUnitOfWork<>(messages);
        RuntimeException taskException = new RuntimeException("task exception");
        RuntimeException commitException = new RuntimeException("commit exception");
        RuntimeException cleanupException = new RuntimeException("cleanup exception");
        subject.onCleanup(u -> cleanupCounter.incrementAndGet());
        subject.onCleanup(u -> {
            throw cleanupException;
        });
        subject.onCleanup(u -> cleanupCounter.incrementAndGet());

        try {
            subject.executeWithResult(() -> {
                registerListeners(subject);
                if (subject.getMessage() instanceof DomainEvent domainEvent) {
                    if (domainEvent.aggregateId().equals("2")) {
                        subject.addHandler(PREPARE_COMMIT, u -> {
                            throw commitException;
                        });
                        throw taskException;
                    }
                }
                return resultFor(subject.getMessage());
            }, e -> false);
        } catch (Exception ignored) {
        }
        validatePhaseTransitions(Arrays.asList(PREPARE_COMMIT, ROLLBACK, CLEANUP), messages);
        Map<DomainEvent, ResultMessage<DomainEvent>> expectedResult = new HashMap<>();
        expectedResult.put(messages.get(0), ResultMessage.failure(commitException));
        expectedResult.put(messages.get(1), ResultMessage.failure(commitException));
        expectedResult.put(messages.get(2), ResultMessage.failure(taskException));
        assertExecutionResults(expectedResult, subject.getExecutionResults());
        assertSame(commitException, taskException.getSuppressed()[0]);
        assertEquals(2, cleanupCounter.get());
    }

    private void registerListeners(UnitOfWork<?> unitOfWork) {
        unitOfWork.onPrepareCommit(u -> transitions.add(new PhaseTransition(u.getMessage(), PREPARE_COMMIT)));
        unitOfWork.onCommit(u -> transitions.add(new PhaseTransition(u.getMessage(), COMMIT)));
        unitOfWork.afterCommit(u -> transitions.add(new PhaseTransition(u.getMessage(), AFTER_COMMIT)));
        unitOfWork.onRollback(u -> transitions.add(new PhaseTransition(u.getMessage(), ROLLBACK)));
        unitOfWork.onCleanup(u -> transitions.add(new PhaseTransition(u.getMessage(), CLEANUP)));
    }

    private static DomainEvent toMessage(Object payload) {
        return new DomainEventTest(Objects.toString(payload));
    }

    public static Object resultFor(DomainEvent message) {
        return message;
    }

    private void validatePhaseTransitions(List<UnitOfWork.Phase> phases, List<DomainEvent> messages) {
        Iterator<PhaseTransition> iterator = transitions.iterator();
        for (UnitOfWork.Phase phase : phases) {
            Iterator<DomainEvent> messageIterator = phase.isReverseCallbackOrder()
                    ? new LinkedList<>(messages).descendingIterator() : messages.iterator();
            messageIterator.forEachRemaining(message -> {
                PhaseTransition expected = new PhaseTransition(message, phase);
                assertTrue(iterator.hasNext(), "Iterator should have next but was empty!");
                PhaseTransition actual = iterator.next();
                assertEquals(expected, actual);
            });
        }
    }

    private void assertExecutionResults(Map<DomainEvent, ResultMessage<DomainEvent>> expected,
                                        Map<DomainEvent, ResultMessage<DomainEvent>> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        List<ResultMessage<?>> expectedMessages = new ArrayList<>(expected.values());
        List<ResultMessage<?>> actualMessages = new ArrayList<>(actual.values());
        List<?> expectedPayloads = expectedMessages.stream()
                .filter(crm -> !crm.isExceptional())
                .map(ResultMessage::getResult)
                .toList();
        List<?> actualPayloads = actualMessages.stream()
                .filter(crm -> !crm.isExceptional())
                .map(ResultMessage::getResult)
                .toList();
        List<Throwable> expectedExceptions = expectedMessages.stream()
                .filter(ResultMessage::isExceptional)
                .map(ResultMessage::getExceptionResult)
                .toList();
        List<Throwable> actualExceptions = actualMessages.stream()
                .filter(ResultMessage::isExceptional)
                .map(ResultMessage::getExceptionResult)
                .toList();

        assertEquals(expectedPayloads.size(), actualPayloads.size());
        expectedPayloads.forEach(it -> {
            final var aResult = actualPayloads.stream().anyMatch(it2 -> {
                if (it2 instanceof DomainEvent domainEvent) {
                    return domainEvent.aggregateId().equals(((DomainEvent) it).aggregateId());
                } else {
                    return false;
                }
            });

            assertTrue(aResult);
        });

        assertEquals(expectedExceptions.size(), actualExceptions.size());
        assertTrue(expectedExceptions.containsAll(actualExceptions));
    }

    private record DomainEventTest(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId
    ) implements DomainEvent {

        public DomainEventTest(String aggregateId) {
            this(UUID.randomUUID().toString(), "DomainEventTest", Instant.now(), aggregateId, 1, "DomainEventTest", UUID.randomUUID().toString());
        }
    }

    private static class PhaseTransition {

        private final UnitOfWork.Phase phase;
        private final Object message;

        public PhaseTransition(Object message, UnitOfWork.Phase phase) {
            this.message = message;
            this.phase = phase;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PhaseTransition that = (PhaseTransition) o;
            return phase == that.phase &&
                    Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, message);
        }

        @Override
        public String toString() {
            return phase + " -> " + message;
        }
    }
}
