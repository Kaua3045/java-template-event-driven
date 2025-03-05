package com.kaua.event.driven.infrastructure.uow;

import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

public interface UnitOfWork<T> {

    void start();

    void commit();

    default void rollback() {
        rollback(null);
    }

    void rollback(Throwable cause);

    default boolean isActive() {
        return phase().isStarted();
    }

    Phase phase();

    void onPrepareCommit(Consumer<UnitOfWork<T>> handler);

    void onCommit(Consumer<UnitOfWork<T>> handler);

    void afterCommit(Consumer<UnitOfWork<T>> handler);

    void onRollback(Consumer<UnitOfWork<T>> handler);

    void onCleanup(Consumer<UnitOfWork<T>> handler);

    Optional<UnitOfWork<?>> parent();

    default boolean isRoot() {
        return !parent().isPresent();
    }

    default UnitOfWork<?> root() {
        //noinspection unchecked // cast is used to remove inspection error in IDE
        return parent().map(UnitOfWork::root).orElse((UnitOfWork) this);
    }

    T getMessage();

//    UnitOfWork<T> transformMessage(Function<T, ? extends Message<?>> transformOperator);

    Map<String, Object> resources();

    @SuppressWarnings("unchecked")
    default <R> R getResource(String name) {
        return (R) resources().get(name);
    }

    @SuppressWarnings("unchecked")
    default <R> R getOrComputeResource(String key, Function<? super String, R> mappingFunction) {
        return (R) resources().computeIfAbsent(key, mappingFunction);
    }

    @SuppressWarnings("unchecked")
    default <R> R getOrDefaultResource(String key, R defaultValue) {
        return (R) resources().getOrDefault(key, defaultValue);
    }

    default void attachTransaction(TransactionManager transactionManager) {
        try {
            Transaction transaction = transactionManager.startTransaction();
            onCommit(u -> transaction.commit());
            onRollback(u -> transaction.rollback());
        } catch (Throwable t) {
            rollback(t);
            throw t;
        }
    }

    default void execute(Runnable task) {
        execute(task, RollbackConfigurationType.ANY_THROWABLE);
    }

    default void execute(Runnable task, RollbackConfiguration rollbackConfiguration) {
        ResultMessage<?> resultMessage = executeWithResult(() -> {
            task.run();
            return null;
        }, rollbackConfiguration);
        if (resultMessage.isExceptional()) {
            throw (RuntimeException) resultMessage.getExceptionResult();
        }
    }

    default <R> ResultMessage<R> executeWithResult(Callable<R> task) {
        return executeWithResult(task, RollbackConfigurationType.ANY_THROWABLE);
    }

    <R> ResultMessage<R> executeWithResult(Callable<R> task, @Nonnull RollbackConfiguration rollbackConfiguration);

    boolean isRolledBack();

    default boolean isCurrent() {
        return CurrentUnitOfWork.isStarted() && CurrentUnitOfWork.get() == this;
    }

    enum Phase {

        NOT_STARTED(false, false, false),
        STARTED(true, false, false),
        PREPARE_COMMIT(true, false, false),
        COMMIT(true, true, false),
        ROLLBACK(true, true, true),
        AFTER_COMMIT(true, true, true),
        CLEANUP(false, true, true),
        CLOSED(false, true, true);

        private final boolean started;
        private final boolean reverseCallbackOrder;
        private final boolean suppressHandlerErrors;

        Phase(boolean started, boolean reverseCallbackOrder, boolean suppressHandlerErrors) {
            this.started = started;
            this.reverseCallbackOrder = reverseCallbackOrder;
            this.suppressHandlerErrors = suppressHandlerErrors;
        }

        public boolean isStarted() {
            return started;
        }
        public boolean isReverseCallbackOrder() {
            return reverseCallbackOrder;
        }
        public boolean isSuppressHandlerErrors() {
            return suppressHandlerErrors;
        }
        public boolean isBefore(Phase phase) {
            return ordinal() < phase.ordinal();
        }
        public boolean isAfter(Phase phase) {
            return ordinal() > phase.ordinal();
        }
    }
}
