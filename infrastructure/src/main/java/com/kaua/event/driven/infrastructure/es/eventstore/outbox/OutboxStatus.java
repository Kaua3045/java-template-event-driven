package com.kaua.event.driven.infrastructure.es.eventstore.outbox;

public enum OutboxStatus {

    PENDING,
    SENT,
    FAILED
}
