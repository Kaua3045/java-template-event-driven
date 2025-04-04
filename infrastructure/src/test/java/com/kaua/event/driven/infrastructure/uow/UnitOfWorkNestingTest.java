package com.kaua.event.driven.infrastructure.uow;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class UnitOfWorkNestingTest extends UnitTest {

    private List<PhaseTransition> phaseTransitions = new ArrayList<>();
    private UnitOfWork<?> outer;
    private UnitOfWork<?> middle;
    private UnitOfWork<?> inner;

    @SuppressWarnings({"unchecked"})
    @BeforeEach
    void setUp() {
        phaseTransitions.clear();
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }

        outer = new DefaultUnitOfWork(new DomainEventTest("Input 1")) {
            @Override
            public String toString() {
                return "outer";
            }
        };
        middle = new DefaultUnitOfWork(new DomainEventTest("Input middle")) {
            @Override
            public String toString() {
                return "middle";
            }
        };
        inner = new DefaultUnitOfWork(new DomainEventTest("Input 2")) {
            @Override
            public String toString() {
                return "inner";
            }
        };
        registerListeners(outer);
        registerListeners(middle);
        registerListeners(inner);
    }

    private void registerListeners(UnitOfWork<?> unitOfWork) {
        unitOfWork.onPrepareCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.PREPARE_COMMIT)));
        unitOfWork.onCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.COMMIT)));
        unitOfWork.afterCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.AFTER_COMMIT)));
        unitOfWork.onRollback(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.ROLLBACK)));
        unitOfWork.onCleanup(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.CLEANUP)));
    }

    @AfterEach
    void tearDown() {
        assertFalse(CurrentUnitOfWork.isStarted(), "A UnitOfWork was not properly cleared");
    }

    @Test
    void innerUnitOfWorkNotifiedOfOuterCommitFailure() {
        outer.onPrepareCommit(u -> {
            inner.start();
            inner.commit();
        });
        outer.onCommit(u -> {
            throw new RuntimeException();
        });
        outer.onCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.COMMIT, "x")));
        outer.start();
        try {
            outer.commit();
        } catch (RuntimeException e) {
            //ok
        }

        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.COMMIT, "x"),
                new PhaseTransition(inner, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void innerUnitOfWorkNotifiedOfOuterPrepareCommitFailure() {
        outer.onPrepareCommit(u -> {
            inner.start();
            inner.commit();
        });
        outer.onPrepareCommit(u -> {
            throw new RuntimeException();
        });
        outer.start();
        try {
            outer.commit();
        } catch (RuntimeException e) {
            //ok
        }

        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void innerUnitOfWorkNotifiedOfOuterCommit() {
        outer.onPrepareCommit(u -> {
            inner.start();
            inner.commit();
        });
        outer.start();
        outer.commit();

        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void innerUnitRollbackDoesNotAffectOuterCommit() {
        outer.onPrepareCommit(u -> {
            inner.start();
            inner.rollback(new RuntimeException());
        });
        outer.start();
        outer.commit();

        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void rollbackOfMiddleUnitOfWorkRollsBackInner() {
        outer.onPrepareCommit(u -> {
            middle.start();
            inner.start();
            inner.commit();
            middle.rollback();
        });
        outer.start();
        outer.commit();

        assertTrue(middle.isRolledBack(), "The middle UnitOfWork hasn't been correctly marked as rolled back");
        assertTrue(inner.isRolledBack(), "The inner UnitOfWork hasn't been correctly marked as rolled back");
        assertFalse(outer.isRolledBack(), "The out UnitOfWork has been incorrectly marked as rolled back");
        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.COMMIT),
                // important that the inner has been given a rollback signal
                new PhaseTransition(inner, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(middle, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(middle, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
    }

    @Test
    void innerUnitCommitFailureDoesNotAffectOuterCommit() {
        outer.onPrepareCommit(u -> {
            inner.start();
            inner.onCommit(uow -> {
                throw new RuntimeException();
            });
            // commits are invoked in reverse order, so we expect to see this one, but not the previously registered handler
            inner.onCommit(uow -> phaseTransitions.add(new PhaseTransition(inner, UnitOfWork.Phase.COMMIT, "x")));
            try {
                inner.commit();
            } catch (RuntimeException e) {
                //ok
            }
        });
        outer.start();
        outer.commit();

        assertFalse(CurrentUnitOfWork.isStarted(), "The UnitOfWork hasn't been correctly cleared");
        assertEquals(Arrays.asList(new PhaseTransition(outer, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.PREPARE_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.COMMIT, "x"),
                new PhaseTransition(inner, UnitOfWork.Phase.ROLLBACK),
                new PhaseTransition(outer, UnitOfWork.Phase.COMMIT),
                new PhaseTransition(outer, UnitOfWork.Phase.AFTER_COMMIT),
                new PhaseTransition(inner, UnitOfWork.Phase.CLEANUP),
                new PhaseTransition(outer, UnitOfWork.Phase.CLEANUP)
        ), phaseTransitions);
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
        private final UnitOfWork<?> unitOfWork;
        private final String id;

        public PhaseTransition(UnitOfWork<?> unitOfWork, UnitOfWork.Phase phase) {
            this(unitOfWork, phase, "");
        }

        public PhaseTransition(UnitOfWork<?> unitOfWork, UnitOfWork.Phase phase, String id) {
            this.unitOfWork = unitOfWork;
            this.phase = phase;
            this.id = id.length() > 0 ? " " + id : id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PhaseTransition that = (PhaseTransition) o;
            return Objects.equals(phase, that.phase) &&
                    Objects.equals(unitOfWork, that.unitOfWork) &&
                    Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, unitOfWork, id);
        }

        @Override
        public String toString() {
            return unitOfWork + " " + phase + id;
        }
    }
}
