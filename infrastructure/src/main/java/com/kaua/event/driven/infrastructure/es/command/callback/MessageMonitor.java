package com.kaua.event.driven.infrastructure.es.command.callback;

public interface MessageMonitor {

    interface MonitorCallback {
        void reportSuccess();

        void reportFailure(Throwable cause);

        void reportIgnored();
    }
}
