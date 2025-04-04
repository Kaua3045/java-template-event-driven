package com.kaua.event.driven.infrastructure.uow;

import jakarta.annotation.Nonnull;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BatchingUnitOfWork<T> extends AbstractUnitOfWork<T> {

    private final List<ProcessingContext<T>> processingContexts;
    private ProcessingContext<T> processingContext;

    @SafeVarargs
    public BatchingUnitOfWork(T... messages) {
        this(Arrays.asList(messages));
    }

    public BatchingUnitOfWork(List<T> messages) {
        processingContexts = messages.stream().map(ProcessingContext::new).collect(Collectors.toList());
        processingContext = processingContexts.getFirst();
    }

    @Override
    public <R> ResultMessage<R> executeWithResult(Callable<R> task,
                                                  @Nonnull RollbackConfiguration rollbackConfiguration) {
        if (phase() == Phase.NOT_STARTED) {
            start();
        }
        Assert.state(phase() == Phase.STARTED,
                () -> String.format("The UnitOfWork has an incompatible phase: %s", phase()));

        R result = null;
        ResultMessage<R> resultMessage = (ResultMessage<R>) result;
        Exception cause = null;
        for (ProcessingContext<T> processingContext : processingContexts) {
            this.processingContext = processingContext;
            try {
                result = task.call();
                if (result instanceof ResultMessage) {
                    resultMessage = (ResultMessage<R>) result;
                } else {
                    resultMessage = ResultMessage.success(result);
                }
            } catch (Exception e) {
                if (rollbackConfiguration.rollBackOn(e)) {
                    rollback(e);
                    return asResultMessage(e);
                }
                setExecutionResult(asResultMessage(e));
                if (cause != null) {
                    cause.addSuppressed(e);
                } else {
                    cause = e;
                }
                resultMessage = asResultMessage(cause);
                continue;
            }
            setExecutionResult(resultMessage);
        }

        try {
            commit();
        } catch (Exception e) {
            resultMessage = asResultMessage(e);
        }
        return resultMessage;
    }

    public Map<T, ResultMessage<T>> getExecutionResults() {
        return processingContexts.stream().collect(
                Collectors.toMap(ProcessingContext::getMessage, ProcessingContext::getExecutionResult));
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
    protected void setExecutionResult(ResultMessage<?> executionResult) {
        processingContext.setExecutionResult((ResultMessage<T>) executionResult);
    }

    @Override
    protected void notifyHandlers(Phase phase) {
        Iterator<ProcessingContext<T>> iterator =
                phase.isReverseCallbackOrder() ? new LinkedList<>(processingContexts).descendingIterator() :
                        processingContexts.iterator();
        iterator.forEachRemaining(context -> (processingContext = context).notifyHandlers(this, phase));
    }

    @Override
    protected void setRollbackCause(Throwable cause) {
        processingContexts.forEach(context -> context
                .setExecutionResult(ResultMessage.failure(cause)));
    }

    @Override
    protected void addHandler(Phase phase, Consumer<UnitOfWork<T>> handler) {
        processingContext.addHandler(phase, handler);
    }

    public List<? extends T> getMessages() {
        return processingContexts.stream().map(ProcessingContext::getMessage).collect(Collectors.toList());
    }

    public boolean isLastMessage(T message) {
        return processingContexts.get(processingContexts.size() - 1).getMessage().equals(message);
    }

    public boolean isLastMessage() {
        return isLastMessage(getMessage());
    }

    public boolean isFirstMessage(T message) {
        return processingContexts.get(0).getMessage().equals(message);
    }

    public boolean isFirstMessage() {
        return isFirstMessage(getMessage());
    }

    private <R> ResultMessage<R> asResultMessage(Exception e) {
        return ResultMessage.failure(e);
    }
}
