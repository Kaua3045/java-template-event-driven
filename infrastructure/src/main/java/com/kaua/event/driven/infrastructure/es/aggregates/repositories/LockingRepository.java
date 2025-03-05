package com.kaua.event.driven.infrastructure.es.aggregates.repositories;

import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.LockAwareAggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.lock.Lock;
import com.kaua.event.driven.infrastructure.es.lock.LockFactory;
import com.kaua.event.driven.infrastructure.es.lock.NoOpLock;
import com.kaua.event.driven.infrastructure.exceptions.ConcurrencyException;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class LockingRepository<T, A extends Aggregate<T>> extends
        AbstractRepository<T, LockAwareAggregate<T, A>> {

    private static final Logger log = LoggerFactory.getLogger(LockingRepository.class);

    private final LockFactory lockFactory;

    public LockingRepository(AggregateModel<T> aggregateModel, LockFactory lockFactory) {
        super(aggregateModel);
        this.lockFactory = lockFactory;
    }

    @Override
    protected LockAwareAggregate<T, A> doCreateNew(Callable<T> factoryMethod) throws Exception {
        UnitOfWork<?> uow = CurrentUnitOfWork.get();
        A aggregate = doCreateNewForLock(factoryMethod);
        final String aggregateIdentifier = aggregate.identifierAsString();

        Supplier<Lock> lockSupplier;
        if (!Objects.isNull(aggregateIdentifier)) {
            log.debug("Creating new aggregate {} instance. Lock will be released after creation", aggregateIdentifier);
            Lock lock = this.lockFactory.obtainLock(aggregateIdentifier);
            uow.onCleanup(u -> lock.release());
            lockSupplier = () -> lock;
            log.info("Created new aggregate {} instance. Lock released after creation", aggregateIdentifier);
        } else {
            log.debug("Creating new aggregate instance. No lock will be created");
            lockSupplier = sameInstanceSupplier(() -> {
                Lock lock = Objects.isNull(aggregate.identifierAsString())
                        ? NoOpLock.INSTANCE
                        : this.lockFactory.obtainLock(aggregate.identifierAsString());
                uow.onCleanup(u -> lock.release());
                return lock;
            });
            log.info("Created new aggregate instance. No lock created");
        }

        return new LockAwareAggregate<>(aggregate, lockSupplier);
    }

    @Override
    protected LockAwareAggregate<T, A> doLoad(String aggregateIdentifier) {
        Lock lock = this.lockFactory.obtainLock(aggregateIdentifier);

        try {
            log.debug("Trying to load aggregate {} instance. Lock will be released after loading aggregate", aggregateIdentifier);
            final A aggregate = doLoadWithLock(aggregateIdentifier);
            log.info("Loaded aggregate {} instance. Lock released after loading aggregate", aggregateIdentifier);
            CurrentUnitOfWork.get().onCleanup(u -> lock.release());
            return new LockAwareAggregate<>(aggregate, lock);
        } catch (Throwable ex) {
            log.error("Exception occurred while trying to load an aggregate. Releasing lock.", ex);
            lock.release();
            throw ex;
        }
    }

    @Override
    protected LockAwareAggregate<T, A> doLoad(String aggregateIdentifier, Long expectedVersion) {
        Lock lock = this.lockFactory.obtainLock(aggregateIdentifier);

        try {
            log.debug("Trying to load aggregate {} instance with version {}. Lock will be released after loading", aggregateIdentifier, expectedVersion);
            final A aggregate = doLoadWithLock(aggregateIdentifier, expectedVersion);
            log.info("Loaded aggregate {} instance with version {}. Lock released after loading", aggregateIdentifier, expectedVersion);
            CurrentUnitOfWork.get().onCleanup(u -> lock.release());
            return new LockAwareAggregate<>(aggregate, lock);
        } catch (Throwable ex) {
            log.error("Exception occurred while trying to load an aggregate. Releasing lock.", ex);
            lock.release();
            throw ex;
        }
    }

    @Override
    protected LockAwareAggregate<T, A> doLoadOrCreate(String aggregateIdentifier, Callable<T> factoryMethod) throws Exception {
        Lock lock = this.lockFactory.obtainLock(aggregateIdentifier);

        try {
            log.debug("Trying to load aggregate {} instance. Lock will be released after loading", aggregateIdentifier);
            final A aggregate = doLoadWithLock(aggregateIdentifier);
            log.info("Loaded aggregate {} instance. Lock released after loading", aggregateIdentifier);
            CurrentUnitOfWork.get().onCleanup(u -> lock.release());
            return new LockAwareAggregate<>(aggregate, lock);
        } catch (NotFoundException ex) {
            log.debug("Aggregate {} not found. Creating new instance. Lock will be released after creation", aggregateIdentifier);
            final A aggregate = doCreateNewForLock(factoryMethod);
            log.info("Created new aggregate {} instance. Lock released after creation", aggregateIdentifier);
            CurrentUnitOfWork.get().onCleanup(u -> lock.release());
            return new LockAwareAggregate<>(aggregate, lock);
        } catch (Throwable ex) {
            log.error("Exception occurred while trying to load an aggregate. Releasing lock", ex);
            lock.release();
            throw ex;
        }
    }

    @Override
    protected void doSave(LockAwareAggregate<T, A> aggregate) {
        if (aggregate.version() != null && !aggregate.isLockHeld()) {
            throw new ConcurrencyException(String.format(
                    "The aggregate of type [%s] with identifier [%s] could not be " +
                            "saved, as a valid lock is not held. Either another thread has saved an aggregate, or " +
                            "the current thread had released its lock earlier on.",
                    aggregate.getClass().getSimpleName(), aggregate.identifierAsString()));
        }
        doSaveWithLock(aggregate.getWrappedAggregate());
    }

    @Override
    protected void doDelete(LockAwareAggregate<T, A> aggregate) {
        if (aggregate.version() != null && !aggregate.isLockHeld()) {
            throw new ConcurrencyException(String.format(
                    "The aggregate of type [%s] with identifier [%s] could not be " +
                            "deleted, as a valid lock is not held. Either another thread has saved an aggregate, or " +
                            "the current thread had released its lock earlier on.",
                    aggregate.getClass().getSimpleName(), aggregate.identifierAsString()));
        }
        doDeleteWithLock(aggregate.getWrappedAggregate());
    }

    @Override
    protected void prepareForCommit(LockAwareAggregate<T, A> aggregate) {
        Assert.state(aggregate.isLockHeld(), () -> "An aggregate is being used for which a lock is no longer held");
        super.prepareForCommit(aggregate);
    }

    protected abstract A doCreateNewForLock(Callable<T> factoryMethod) throws Exception;

    protected abstract A doLoadWithLock(String aggregateIdentifier);

    protected abstract A doLoadWithLock(String aggregateIdentifier, Long expectedVersion);

    protected abstract void doSaveWithLock(A aggregate);

    protected abstract void doDeleteWithLock(A aggregate);

    private <S> Supplier<S> sameInstanceSupplier(Supplier<S> supplier) {
        AtomicReference<S> instanceRef = new AtomicReference<>();
        // Using the AtomicReference ensures the lock is only created once for the supplier's invocations.
        return () -> instanceRef.updateAndGet(current -> getOrDefault(current, supplier));
    }

    private <S> S getOrDefault(S instance, Supplier<S> defaultProvider) {
        if (instance == null) {
            return defaultProvider.get();
        }
        return instance;
    }
}
