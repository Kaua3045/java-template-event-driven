package com.kaua.event.driven.utils;

import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateRoot;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateVersion;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventSourcingHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.TargetAggregateIdentifier;

@AggregateRoot
public class StubAggregate {

    @TargetAggregateIdentifier
    private String id;

    @AggregateVersion
    private long version;

    public StubAggregate() {
        this.id = IdentifierUtils.generateNewId();
        this.version = 0L;
    }

    public StubAggregate(String id, long version) {
        this.id = id;
        this.version = version;
    }

    public StubAggregate(String id) {
        this.id = id;
        this.version = 0L;
    }

    public void doSomething() {
        AggregateLifecycle.apply(new StubDomainEvent(id, "payload"));
    }

    @EventSourcingHandler
    public void on(StubDomainEvent event) {
        this.id = event.aggregateId();
        this.version++;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}