package com.kaua.event.driven.infrastructure.uow;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AbstractUnitOfWorkTest extends UnitTest {

    private List<PhaseTransition> phaseTransitions;
    private UnitOfWork<?> subject;

    @SuppressWarnings({"unchecked"})
    @BeforeEach
    void setUp() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
        subject = spy(new DefaultUnitOfWork(new CommandTest(IdentifierUtils.generateNewId())) {
            @Override
            public String toString() {
                return "unitOfWork";
            }
        });
        phaseTransitions = new ArrayList<>();
        registerListeners(subject);
    }

    @AfterEach
    void tearDown() {
        assertFalse(CurrentUnitOfWork.isStarted(), "A UnitOfWork was not properly cleared");
    }

    @Test
    void start_ShouldThrowException_WhenCalledTwice() {
        subject.start();
        IllegalStateException exception = assertThrows(IllegalStateException.class, subject::start);
        assertEquals("UnitOfWork is already started", exception.getMessage());
        CurrentUnitOfWork.clear(subject);
    }

    @Test
    void commit_ShouldThrowException_WhenCalledBeforeStart() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, subject::commit);
        assertEquals("The UnitOfWork is in an incompatible phase: NOT_STARTED", exception.getMessage());
    }

    @Test
    void commit_ShouldThrowException_WhenNotCurrent() {
        subject.start();
        UnitOfWork anotherUow = new DefaultUnitOfWork(new CommandTest(IdentifierUtils.generateNewId()));
        anotherUow.start(); // Isso faz com que subject não seja o atual

        IllegalStateException exception = assertThrows(IllegalStateException.class, subject::commit);
        assertEquals("The UnitOfWork is not the current Unit of Work", exception.getMessage());
        CurrentUnitOfWork.clear(anotherUow);
        CurrentUnitOfWork.clear(subject);
    }

    @Test
    void rollback_ShouldThrowException_WhenNotCurrent() {
        subject.start();
        UnitOfWork anotherUow = new DefaultUnitOfWork(new CommandTest(IdentifierUtils.generateNewId()));
        anotherUow.start(); // Isso faz com que subject não seja o atual

        IllegalStateException exception = assertThrows(IllegalStateException.class, subject::rollback);
        assertEquals("The UnitOfWork is not the current Unit of Work", exception.getMessage());
        CurrentUnitOfWork.clear(anotherUow);
        CurrentUnitOfWork.clear(subject);
    }

    @Test
    void rollback_ShouldThrowException_WhenAlreadyRolledBack() {
        subject.start();
        subject.rollback();

        IllegalStateException exception = assertThrows(IllegalStateException.class, subject::rollback);
        assertEquals("The UnitOfWork is in an incompatible phase: CLOSED", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRollbackCalledInInvalidPhase() {
        UnitOfWork<?> unitOfWork = new DefaultUnitOfWork<>(new CommandTest(IdentifierUtils.generateNewId()));
        unitOfWork.start();

        unitOfWork.rollback(new RuntimeException());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                unitOfWork.rollback(new RuntimeException("Test error"))
        );

        assertEquals("The UnitOfWork is in an incompatible phase: CLOSED", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRollbackCalledInInvalidActive() {
        UnitOfWork<?> unitOfWork = new DefaultUnitOfWork<>(new CommandTest(IdentifierUtils.generateNewId()));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                unitOfWork.rollback(new RuntimeException("Test error"))
        );

        assertEquals("The UnitOfWork is in an incompatible phase: NOT_STARTED", exception.getMessage());
    }

    @Test
    void testHandlersForCurrentPhaseAreExecuted() {
        AtomicBoolean prepareCommit = new AtomicBoolean();
        AtomicBoolean commit = new AtomicBoolean();
        AtomicBoolean afterCommit = new AtomicBoolean();
        AtomicBoolean cleanup = new AtomicBoolean();
        subject.onPrepareCommit(u -> subject.onPrepareCommit(i -> prepareCommit.set(true)));
        subject.onCommit(u -> subject.onCommit(i -> commit.set(true)));
        subject.afterCommit(u -> subject.afterCommit(i -> afterCommit.set(true)));
        subject.onCleanup(u -> subject.onCleanup(i -> cleanup.set(true)));

        subject.start();
        subject.commit();

        assertTrue(prepareCommit.get());
        assertTrue(commit.get());
        assertTrue(afterCommit.get());
        assertTrue(cleanup.get());
    }

    @Test
    void testExecuteTaskInUnitOfWork() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        subject.execute(task);
        InOrder inOrder = inOrder(task, subject);
        inOrder.verify(subject).start();
        inOrder.verify(task).run();
        inOrder.verify(subject).commit();
        assertFalse(subject.isActive());
    }

    @Test
    void testExecuteFailingTaskInUnitOfWork() {
        Runnable task = mock(Runnable.class);
        RuntimeException mockException = new RuntimeException();
        doThrow(mockException).when(task).run();
        try {
            subject.execute(task);
        } catch (RuntimeException e) {
            InOrder inOrder = inOrder(task, subject);
            inOrder.verify(subject).start();
            inOrder.verify(task).run();
            inOrder.verify(subject).rollback(e);
            assertNotNull(subject.getResultMessage());
            assertSame(mockException, subject.getResultMessage().getExceptionResult());
            return;
        }
        throw new AssertionError();
    }

    @Test
    void testExecuteTaskWithResultInUnitOfWork() throws Exception {
        Object taskResult = new Object();
        Callable<Object> task = mock(Callable.class);
        when(task.call()).thenReturn(taskResult);
        ResultMessage result = subject.executeWithResult(task);
        InOrder inOrder = inOrder(task, subject);
        inOrder.verify(subject).start();
        inOrder.verify(task).call();
        inOrder.verify(subject).commit();
        assertFalse(subject.isActive());
        assertSame(taskResult, result.getResult());
        assertNotNull(subject.getResultMessage());
        assertSame(taskResult, subject.getResultMessage().getResult());
    }

    @Test
    void testExecuteTaskReturnsResultMessageInUnitOfWork() throws Exception {
        ResultMessage<Object> resultMessage = ResultMessage.success(new Object());
        Callable<ResultMessage<Object>> task = mock(Callable.class);
        when(task.call()).thenReturn(resultMessage);
        ResultMessage actualResultMessage = subject.executeWithResult(task);
        assertSame(resultMessage, actualResultMessage);
    }

    @Test
    void testAttachedTransactionCommittedOnUnitOfWorkCommit() {
        TransactionManager transactionManager = mock(TransactionManager.class);
        Transaction transaction = mock(Transaction.class);
        when(transactionManager.startTransaction()).thenReturn(transaction);
        subject.attachTransaction(transactionManager);
        subject.start();
        verify(transactionManager).startTransaction();
        verify(transaction, never()).commit();
        subject.commit();
        verify(transaction).commit();
    }

    @Test
    void testAttachedTransactionRolledBackOnUnitOfWorkRollBack() {
        TransactionManager transactionManager = mock(TransactionManager.class);
        Transaction transaction = mock(Transaction.class);
        when(transactionManager.startTransaction()).thenReturn(transaction);
        subject.attachTransaction(transactionManager);
        subject.start();
        verify(transactionManager).startTransaction();
        verify(transaction, never()).commit();
        verify(transaction, never()).rollback();

        subject.rollback();
        verify(transaction).rollback();
        verify(transaction, never()).commit();

    }

    @Test
    void testUnitOfWorkIsRolledBackWhenTransactionFailsToStart() {
        TransactionManager transactionManager = mock(TransactionManager.class);
        when(transactionManager.startTransaction()).thenThrow(new RuntimeException());
        try {
            subject.attachTransaction(transactionManager);
            fail("Expected RuntimeException to be propagated");
        } catch (Exception e) {
            // expected
        }
        verify(subject).rollback(isA(RuntimeException.class));
    }

    private void registerListeners(UnitOfWork<?> unitOfWork) {
        unitOfWork.onPrepareCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.PREPARE_COMMIT)));
        unitOfWork.onCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.COMMIT)));
        unitOfWork.afterCommit(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.AFTER_COMMIT)));
        unitOfWork.onRollback(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.ROLLBACK)));
        unitOfWork.onCleanup(u -> phaseTransitions.add(new PhaseTransition(u, UnitOfWork.Phase.CLEANUP)));
    }

    private record CommandTest(
            String commandId,
            String commandType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String traceId

    ) implements Command {

        public CommandTest(String aggregateId) {
            this("commandId", "commandType", Instant.now(), aggregateId, 1, "traceId");
        }
    }

    private record PhaseTransition(UnitOfWork<?> unitOfWork, UnitOfWork.Phase phase) {

        @Override
        public String toString() {
            return unitOfWork + " " + phase;
        }
    }
}
