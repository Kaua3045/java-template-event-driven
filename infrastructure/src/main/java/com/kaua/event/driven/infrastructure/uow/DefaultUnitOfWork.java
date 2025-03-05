package com.kaua.event.driven.infrastructure.uow;

import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class DefaultUnitOfWork<T> extends AbstractUnitOfWork<T> {

    private static final Logger log = LoggerFactory.getLogger(DefaultUnitOfWork.class);
    private static final Deque EMPTY = new LinkedList<>();

    private final EnumMap<Phase, Deque<Consumer<UnitOfWork<T>>>> handlers = new EnumMap<>(Phase.class);

//    private final Map<Phase, List<Consumer<UnitOfWork<T>>>> handlers = new EnumMap<>(Phase.class);
    private final T message;
    private Throwable rollbackCause;

    public DefaultUnitOfWork(T message) {
        this.message = message;
    }

    public static <T> DefaultUnitOfWork<T> startAndGet(T message) {
        DefaultUnitOfWork<T> uow = new DefaultUnitOfWork<>(message);
        uow.start();
        return uow;
    }

    @Override
    protected void notifyHandlers(Phase phase) {
        log.debug("Notifying handlers for phase {}", phase);
        Deque<Consumer<UnitOfWork<T>>> phaseHandlers = handlers.getOrDefault(phase, EMPTY);
        while (!phaseHandlers.isEmpty()) {
            try {
                phaseHandlers.remove().accept(this);
            } catch (Exception e) {
                if (phase.isSuppressHandlerErrors()) {
                    log.info("An error occurred while handling a {} phase. Error: {}", phase, e.getMessage());
                } else {
                    throw e;
                }
            }
        }
//        List<Consumer<UnitOfWork<T>>> phaseHandlers = handlers.getOrDefault(phase, Collections.emptyList());
//        if (phase.isReverseCallbackOrder()) {
//            Collections.reverse(phaseHandlers);
//        }
//        for (Consumer<UnitOfWork<T>> handler : phaseHandlers) {
//            try {
//                handler.accept(this);
//            } catch (Exception e) {
//                if (!phase.isSuppressHandlerErrors()) {
//                    throw e;
//                }
//            }
//        }
    }

    @Override
    protected void addHandler(Phase phase, Consumer<UnitOfWork<T>> handler) {
        log.debug("Adding handler for phase {}", phase);
        final Deque<Consumer<UnitOfWork<T>>> consumers = handlers.computeIfAbsent(phase, p -> new ArrayDeque<>());
        if (phase.isReverseCallbackOrder()) {
            consumers.addFirst(handler);
        } else {
            consumers.add(handler);
        }
//        handlers.computeIfAbsent(phase, p -> new ArrayList<>()).add(handler);
    }

    @Override
    protected void setRollbackCause(Throwable cause) {
        this.rollbackCause = cause;
    }

    @Override
    public T getMessage() {
        return message;
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
            resultMessage = ResultMessage.success(result);
        } catch (Exception e) {
            resultMessage = asResultMessage(e);
            if (rollbackConfiguration.rollBackOn(e)) {
                rollback(e);
                return resultMessage;
            }
        }
        try {
            commit();
        } catch (Exception e) {
            resultMessage = asResultMessage(e);
        }
        return resultMessage;
    }

    private <R> ResultMessage<R> asResultMessage(Exception e) {
        return ResultMessage.failure(e);
    }
}
