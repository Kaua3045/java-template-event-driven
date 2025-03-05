package com.kaua.event.driven.infrastructure.uow;

public interface RollbackConfiguration {

    boolean rollBackOn(Throwable throwable);
}
