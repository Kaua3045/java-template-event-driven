package com.kaua.event.driven.infrastructure.uow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.function.Consumer;

public abstract class AbstractUnitOfWork<T> implements UnitOfWork<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractUnitOfWork.class);
    private final Map<String, Object> resources = new HashMap<>();
    private UnitOfWork<?> parentUnitOfWork;
    private Phase phase = Phase.NOT_STARTED;
    private boolean rolledBack;

    @Override
    public void start() {
        logger.debug("Starting Unit Of Work");
        Assert.state(Phase.NOT_STARTED.equals(phase()), () -> "UnitOfWork is already started");
        rolledBack = false;
        onRollback(u -> rolledBack = true);
        CurrentUnitOfWork.ifStarted(parent -> {
            // we're nesting.
            this.parentUnitOfWork = parent;
            root().onCleanup(r -> changePhase(Phase.CLEANUP, Phase.CLOSED));
        });
        changePhase(Phase.STARTED);
        CurrentUnitOfWork.set(this);
    }

    @Override
    public void commit() {
        logger.debug("Committing Unit Of Work");
        Assert.state(phase() == Phase.STARTED, () -> String.format("The UnitOfWork is in an incompatible phase: %s", phase()));
        Assert.state(isCurrent(), () -> "The UnitOfWork is not the current Unit of Work");
        try {
            if (isRoot()) {
                commitAsRoot();
            } else {
                commitAsNested();
            }
        } finally {
            CurrentUnitOfWork.clear(this);
        }
    }

    private void commitAsRoot() {
        try {
            try {
                changePhase(Phase.PREPARE_COMMIT, Phase.COMMIT);
            } catch (Exception e) {
                setRollbackCause(e);
                changePhase(Phase.ROLLBACK);
                throw e;
            }
            if (phase() == Phase.COMMIT) {
                changePhase(Phase.AFTER_COMMIT);
            }
        } finally {
            changePhase(Phase.CLEANUP, Phase.CLOSED);
        }
    }

    private void commitAsNested() {
        try {
            changePhase(Phase.PREPARE_COMMIT, Phase.COMMIT);
            delegateAfterCommitToParent(this);
            parentUnitOfWork.onRollback(u -> changePhase(Phase.ROLLBACK));
        } catch (Exception e) {
            setRollbackCause(e);
            changePhase(Phase.ROLLBACK);
            throw e;
        }
    }

    private void delegateAfterCommitToParent(UnitOfWork<?> uow) {
        Optional<UnitOfWork<?>> parent = uow.parent();
        if (parent.isPresent()) {
            parent.get().afterCommit(this::delegateAfterCommitToParent);
        } else {
            changePhase(Phase.AFTER_COMMIT);
        }
    }

    @Override
    public void rollback(Throwable cause) {
        logger.debug("Rolling back Unit Of Work.", cause);
        Assert.state(isActive() && phase().isBefore(Phase.ROLLBACK),
                () -> String.format("The UnitOfWork is in an incompatible phase: %s", phase()));
        Assert.state(isCurrent(), () -> "The UnitOfWork is not the current Unit of Work");
        try {
            setRollbackCause(cause);
            changePhase(Phase.ROLLBACK);
            if (isRoot()) {
                changePhase(Phase.CLEANUP, Phase.CLOSED);
            }
        } finally {
            CurrentUnitOfWork.clear(this);
        }
    }

    @Override
    public Optional<UnitOfWork<?>> parent() {
        return Optional.ofNullable(parentUnitOfWork);
    }

    @Override
    public Map<String, Object> resources() {
        return resources;
    }

    @Override
    public boolean isRolledBack() {
        return rolledBack;
    }

    @Override
    public void onPrepareCommit(Consumer<UnitOfWork<T>> handler) {
        addHandler(Phase.PREPARE_COMMIT, handler);
    }

    @Override
    public void onCommit(Consumer<UnitOfWork<T>> handler) {
        addHandler(Phase.COMMIT, handler);
    }

    @Override
    public void afterCommit(Consumer<UnitOfWork<T>> handler) {
        addHandler(Phase.AFTER_COMMIT, handler);
    }

    @Override
    public void onRollback(Consumer<UnitOfWork<T>> handler) {
        addHandler(Phase.ROLLBACK, handler);
    }

    @Override
    public void onCleanup(Consumer<UnitOfWork<T>> handler) {
        addHandler(Phase.CLEANUP, handler);
    }

    @Override
    public Phase phase() {
        return phase;
    }

    protected void setPhase(Phase phase) {
        this.phase = phase;
    }

    protected void changePhase(Phase... phases) {
        for (Phase phase : phases) {
            setPhase(phase);
            notifyHandlers(phase);
        }
    }

    protected abstract void notifyHandlers(Phase phase);

    protected abstract void addHandler(Phase phase, Consumer<UnitOfWork<T>> handler);

    protected abstract void setExecutionResult(ResultMessage<?> executionResult);

    protected abstract void setRollbackCause(Throwable cause);
}
