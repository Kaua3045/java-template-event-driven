package com.kaua.event.driven.infrastructure.uow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class CurrentUnitOfWork {

    private static final ThreadLocal<Deque<UnitOfWork<?>>> CURRENT = new ThreadLocal<>();
    private static final Logger log = LoggerFactory.getLogger(CurrentUnitOfWork.class);

    public static boolean isStarted() {
        return CURRENT.get() != null && !CURRENT.get().isEmpty();
    }

    public static boolean ifStarted(Consumer<UnitOfWork<?>> consumer) {
        if (isStarted()) {
            consumer.accept(get());
            return true;
        }
        return false;
    }

    public static <T> Optional<T> map(Function<UnitOfWork<?>, T> function) {
        return isStarted() ? Optional.ofNullable(function.apply(get())) : Optional.empty();
    }

    public static UnitOfWork<?> get() {
        if (isEmpty()) {
            throw new IllegalStateException("No UnitOfWork is currently started for this thread.");
        }
        return CURRENT.get().peek();
    }

    private static boolean isEmpty() {
        Deque<UnitOfWork<?>> unitsOfWork = CURRENT.get();
        return unitsOfWork == null || unitsOfWork.isEmpty();
    }

    public static void commit() {
        get().commit();
    }

    public static void set(UnitOfWork<?> unitOfWork) {
        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<>());
        }
        CURRENT.get().push(unitOfWork);
    }

    public static void clear(UnitOfWork<?> unitOfWork) {
        if (!isStarted()) {
            throw new IllegalStateException("Could not clear this UnitOfWork. There is no UnitOfWork active.");
        }
        if (CURRENT.get().peek() == unitOfWork) {
            CURRENT.get().pop();
            if (CURRENT.get().isEmpty()) {
                CURRENT.remove();
                log.debug("Removed the UnitOfWork from the current thread");
            }
        } else {
            throw new IllegalStateException("Could not clear this UnitOfWork. It is not the active one.");
        }
    }

    private CurrentUnitOfWork() {
    }
}
