package com.kaua.event.driven.infrastructure.es.lock;

public enum NullLockFactory implements LockFactory {

    INSTANCE;

    @Override
    public Lock obtainLock(String identifier) {
        return NoOpLock.INSTANCE;
    }
}
