package com.kaua.event.driven.infrastructure.es.command.callback;

import jakarta.annotation.Nonnull;

public enum NoOpMessageMonitor implements MessageMonitor<Object> {

    INSTANCE;

    public static NoOpMessageMonitor instance() {
        return INSTANCE;
    }

    @Override
    public MonitorCallback onMessageIngested(@Nonnull Object message) {
        return NoOpMessageMonitorCallback.INSTANCE;
    }
}
