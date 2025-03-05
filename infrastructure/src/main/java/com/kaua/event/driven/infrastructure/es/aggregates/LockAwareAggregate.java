package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.infrastructure.es.lock.Lock;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class LockAwareAggregate<AR, A extends Aggregate<AR>> implements Aggregate<AR> {

    private final A wrappedAggregate;
    private final LockSupplier lock;

    public LockAwareAggregate(A wrappedAggregate, Lock lock) {
        this.wrappedAggregate = wrappedAggregate;
        this.lock = () -> lock;
    }

    public LockAwareAggregate(A wrappedAggregate, Supplier<Lock> lock) {
        this.wrappedAggregate = wrappedAggregate;
        this.lock = lock::get;
    }

    public A getWrappedAggregate() {
        return wrappedAggregate;
    }

    public boolean isLockHeld() {
        return this.lock.acquire().isHeld();
    }

    @Override
    public String type() {
        return wrappedAggregate.type();
    }

    @Override
    public Object identifier() {
        return wrappedAggregate.identifier();
    }

    @Override
    public Long version() {
        return wrappedAggregate.version();
    }

    @Override
    public Object handle(Object message) {
        Object aResult = this.wrappedAggregate.handle(message);

        this.lock.acquire();
        return aResult;
    }

    @Override
    public <R> R invoke(Function<AR, R> invocation) {
        R aResult = this.wrappedAggregate.invoke(invocation);

        this.lock.acquire();
        return aResult;
    }

    @Override
    public void execute(Consumer<AR> invocation) {
        try {
            this.wrappedAggregate.execute(invocation);
        } finally {
            this.lock.acquire();
        }
    }

    @Override
    public boolean isDeleted() {
        return this.wrappedAggregate.isDeleted();
    }

    @Override
    public Class<? extends AR> rootType() {
        return this.wrappedAggregate.rootType();
    }

    @FunctionalInterface
    private interface LockSupplier extends Supplier<Lock> {

        default Lock acquire() {
            return this.get();
        }
    }
}
