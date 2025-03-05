package com.kaua.event.driven.infrastructure.es.lock;

@FunctionalInterface
public interface LockFactory {

    Lock obtainLock(String identifier);
}
