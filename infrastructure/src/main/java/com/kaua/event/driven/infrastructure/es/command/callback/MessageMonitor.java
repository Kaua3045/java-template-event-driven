package com.kaua.event.driven.infrastructure.es.command.callback;

import jakarta.annotation.Nonnull;

public interface MessageMonitor<T> {

    MonitorCallback onMessageIngested(@Nonnull T message);

    interface MonitorCallback {
        void reportSuccess();

        void reportFailure(Throwable cause);

        void reportIgnored();
    }
}
