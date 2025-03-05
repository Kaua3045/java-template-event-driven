package com.kaua.event.driven.infrastructure.es.lock;

public interface Lock extends AutoCloseable {

    @Override
    default void close() { release(); }

    void release();

    boolean isHeld();
}
