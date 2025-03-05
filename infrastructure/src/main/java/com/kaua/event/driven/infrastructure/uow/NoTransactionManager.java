package com.kaua.event.driven.infrastructure.uow;

public enum NoTransactionManager implements TransactionManager {

    INSTANCE;

    public static TransactionManager instance() {
        return INSTANCE;
    }

    @Override
    public Transaction startTransaction() {
        return TRANSACTION;
    }

    private static final Transaction TRANSACTION = new Transaction() {
        @Override
        public void commit() {
            //no op
        }

        @Override
        public void rollback() {
            //no op
        }
    };
}
