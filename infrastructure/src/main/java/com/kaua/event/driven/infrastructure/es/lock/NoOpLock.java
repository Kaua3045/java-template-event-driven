package com.kaua.event.driven.infrastructure.es.lock;

public class NoOpLock implements Lock {

    public static final Lock INSTANCE = new NoOpLock();

    private NoOpLock() {
    }

    @Override
    public void release() {

    }

    @Override
    public boolean isHeld() {
        return true;
    }
}
