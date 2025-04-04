package com.kaua.event.driven.infrastructure.es.command.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum NoOpMessageMonitorCallback implements MessageMonitor.MonitorCallback {

    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(NoOpMessageMonitorCallback.class);

    @Override
    public void reportSuccess() {
        log.debug("Message processed successfully");
    }

    @Override
    public void reportFailure(Throwable cause) {
        log.debug("Message processing failed", cause);
    }

    @Override
    public void reportIgnored() {
        log.debug("Message ignored");
    }
}
