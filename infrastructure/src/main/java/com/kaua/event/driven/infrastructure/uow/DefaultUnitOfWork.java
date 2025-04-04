package com.kaua.event.driven.infrastructure.uow;

import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class DefaultUnitOfWork<T> extends AbstractUnitOfWork<T> {

    private static final Logger log = LoggerFactory.getLogger(DefaultUnitOfWork.class);

    private final ProcessingContext<T> processingContext;

    public static <T> DefaultUnitOfWork<T> startAndGet(T message) {
        DefaultUnitOfWork<T> uow = new DefaultUnitOfWork<>(message);
        uow.start();
        return uow;
    }

    public DefaultUnitOfWork(T message) {
        processingContext = new ProcessingContext<>(message);
    }

    @Override
    public T getMessage() {
        return processingContext.getMessage();
    }

    @Override
    public ResultMessage<T> getResultMessage() {
        return processingContext.getExecutionResult();
    }

    @Override
    public <R> ResultMessage<R> executeWithResult(Callable<R> task, @Nonnull RollbackConfiguration rollbackConfiguration) {
        if (phase() == Phase.NOT_STARTED) {
            start();
        }
        Assert.state(phase() == Phase.STARTED,
                () -> String.format("The UnitOfWork has an incompatible phase: %s", phase()));

        ResultMessage<R> resultMessage;
        try {
            R result = task.call();
            if (result instanceof ResultMessage<?>) {
                resultMessage = (ResultMessage<R>) result;
            } else {
                resultMessage = ResultMessage.success(result);
            }
        } catch (Exception e) {
            resultMessage = asResultMessage(e);
            if (rollbackConfiguration.rollBackOn(e)) {
                rollback(e);
                return resultMessage;
            }
        }

        setExecutionResult(resultMessage);

        try {
            commit();
        } catch (Exception e) {
            resultMessage = asResultMessage(e);
        }
        return resultMessage;
    }

    @Override
    protected void notifyHandlers(Phase phase) {
        processingContext.notifyHandlers(this, phase);
    }

    @Override
    protected void addHandler(Phase phase, Consumer<UnitOfWork<T>> handler) {
        log.debug("Adding handler for phase {}", phase);
        processingContext.addHandler(phase, handler);
    }

    @Override
    protected void setExecutionResult(ResultMessage<?> executionResult) {
        processingContext.setExecutionResult((ResultMessage<T>) executionResult);
    }

    @Override
    protected void setRollbackCause(Throwable cause) {
        setExecutionResult(ResultMessage.failure(cause));
    }

    private <R> ResultMessage<R> asResultMessage(Exception e) {
        return ResultMessage.failure(e);
    }
}
