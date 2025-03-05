package com.kaua.event.driven.infrastructure.uow;

import jakarta.annotation.Nonnull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Component
public class SpringTransactionManager implements TransactionManager {

    private final PlatformTransactionManager transactionManager;
    private final TransactionDefinition transactionDefinition;

    public SpringTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.transactionDefinition = new DefaultTransactionDefinition();
    }

    @Nonnull
    @Override
    public Transaction startTransaction() {
        TransactionStatus status = transactionManager.getTransaction(transactionDefinition);
        return new Transaction() {
            @Override
            public void commit() {
                commitTransaction(status);
            }

            @Override
            public void rollback() {
                rollbackTransaction(status);
            }
        };
    }

    protected void commitTransaction(TransactionStatus status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            transactionManager.commit(status);
        }
    }

    protected void rollbackTransaction(TransactionStatus status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            transactionManager.rollback(status);
        }
    }
}
