package com.kaua.event.driven.infrastructure.uow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.function.Consumer;

public class ProcessingContext<T> {

    private static final Logger log = LoggerFactory.getLogger(ProcessingContext.class);
    private static final Deque EMPTY = new LinkedList<>();

    private final EnumMap<UnitOfWork.Phase, Deque<Consumer<UnitOfWork<T>>>> handlers = new EnumMap<>(UnitOfWork.Phase.class);
    private T message;
    private ResultMessage<T> executionResult;

    public ProcessingContext(T message) {
        this.message = message;
    }

    public void notifyHandlers(UnitOfWork<T> unitOfWork, UnitOfWork.Phase phase) {
        log.debug("Notifying handlers for phase {}", phase.toString());
        Deque<Consumer<UnitOfWork<T>>> l = handlers.getOrDefault(phase, EMPTY);
        while (!l.isEmpty()) {
            try {
                l.remove().accept(unitOfWork);
            } catch (Exception e) {
                if (phase.isSuppressHandlerErrors()) {
                    log.error("An error occurred while executing a lifecycle phase handler for phase {}", phase, e);
                } else {
                    throw e;
                }
            }
        }
    }

    public void addHandler(UnitOfWork.Phase phase, Consumer<UnitOfWork<T>> handler) {
        log.debug("Adding handler {} for phase {}", handler.getClass().getName(), phase.toString());
        final Deque<Consumer<UnitOfWork<T>>> consumers = handlers.computeIfAbsent(phase, p -> new ArrayDeque<>());
        if (phase.isReverseCallbackOrder()) {
            consumers.addFirst(handler);
        } else {
            consumers.add(handler);
        }
    }

    public void setExecutionResult(ResultMessage<T> executionResult) {
        Assert.state(this.executionResult == null || executionResult.isExceptional(),
                () -> String.format("Cannot change execution result [%s] to [%s] for message [%s].",
                        this.executionResult, executionResult, message));
        if (this.executionResult != null && this.executionResult.isExceptional()) {
            this.executionResult.getExceptionResult().addSuppressed(executionResult.getExceptionResult());
        } else {
            this.executionResult = executionResult;
        }
    }

    public T getMessage() {
        return message;
    }

    public ResultMessage<T> getExecutionResult() {
        return executionResult;
    }

    public void reset(T message) {
        this.message = message;
        handlers.clear();
        executionResult = null;
    }
}
