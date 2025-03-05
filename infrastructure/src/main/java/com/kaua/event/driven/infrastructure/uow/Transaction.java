package com.kaua.event.driven.infrastructure.uow;

public interface Transaction {

    void commit();
    void rollback();
}
