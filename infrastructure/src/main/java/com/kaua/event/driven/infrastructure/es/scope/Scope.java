package com.kaua.event.driven.infrastructure.es.scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

public abstract class Scope {

    private static final Logger log = LoggerFactory.getLogger(Scope.class);

    private static final ThreadLocal<Deque<Scope>> CURRENT_SCOPE = ThreadLocal.withInitial(LinkedList::new);

    public static <S extends Scope> S getCurrentScope() {
        try {
            return (S) CURRENT_SCOPE.get().getFirst();
        } catch (NoSuchElementException ex) {
            throw new IllegalStateException("Cannot request current Scope if none is active");
        }
    }

    public void startScope() {
        log.debug("Starting scope: {}", this);
        CURRENT_SCOPE.get().push(this);
    }

    public void endScope() {
        Deque<Scope> scopes = CURRENT_SCOPE.get();
        if (this != scopes.peek()) {
            throw new IllegalStateException("Incorrectly trying to end another Scope then which the calling process is contained in.");
        }

        scopes.pop();

        if (scopes.isEmpty()) {
            log.debug("Clearing out ThreadLocal current Scope, as no Scopes are present");
            CURRENT_SCOPE.remove();
        }
    }

    protected <V> V executeWithResult(Callable<V> task) throws Exception {
        startScope();
        try {
            return task.call();
        } finally {
            endScope();
        }
    }

    public static ScopeDescriptor describeCurrentScope() {
        return getCurrentScope().describeScope();
    }

    public abstract ScopeDescriptor describeScope();
}
