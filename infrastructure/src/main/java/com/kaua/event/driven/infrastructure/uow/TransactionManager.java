package com.kaua.event.driven.infrastructure.uow;

import java.util.function.Supplier;

public interface TransactionManager {

    Transaction startTransaction();

    default void executeInTransaction(Runnable task) {
        Transaction transaction = startTransaction();
        try {
            task.run();
            transaction.commit();
        } catch (Throwable e) {
            transaction.rollback();
            throw e;
        }
    }

    default <T> T fetchInTransaction(Supplier<T> supplier) {
        Transaction transaction = startTransaction();
        try {
            T result = supplier.get();
            transaction.commit();
            return result;
        } catch (Throwable e) {
            transaction.rollback();
            throw e;
        }
    }
}
