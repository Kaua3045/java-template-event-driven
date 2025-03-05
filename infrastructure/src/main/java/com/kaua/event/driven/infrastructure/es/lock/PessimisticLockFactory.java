package com.kaua.event.driven.infrastructure.es.lock;

import com.kaua.event.driven.infrastructure.exceptions.DeadlockException;
import com.kaua.event.driven.infrastructure.exceptions.LockAcquisitionFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

public class PessimisticLockFactory implements LockFactory {

    private static final Logger log = LoggerFactory.getLogger(PessimisticLockFactory.class);

    private static final Set<PessimisticLockFactory> INSTANCES = synchronizedSet(newSetFromMap(new WeakHashMap<>()));

    private final ConcurrentHashMap<String, DisposableLock> locks = new ConcurrentHashMap<>();
    private final int acquireAttempts;
    private final int maximumQueued;
    private final int lockAttemptTimeout;

    public PessimisticLockFactory(int acquireAttempts, int maximumQueued, int lockAttemptTimeout) {
        this.acquireAttempts = acquireAttempts;
        this.maximumQueued = maximumQueued;
        this.lockAttemptTimeout = lockAttemptTimeout;
        INSTANCES.add(this);
    }

    private static Set<Thread> threadsWaitingForMyLocks(Thread owner) {
        return threadsWaitingForMyLocks(owner, INSTANCES);
    }

    private static Set<Thread> threadsWaitingForMyLocks(Thread owner, Set<PessimisticLockFactory> locksInUse) {
        try {
            Set<Thread> waitingThreads = new HashSet<>();
            locksInUse.stream()
                    .flatMap(lock -> lock.locks.values().stream())
                    .filter(disposableLock -> disposableLock.isHeldBy(owner))
                    .forEach(disposableLock -> disposableLock.queuedThreads().stream()
                            .filter(waitingThreads::add)
                            .forEach(thread -> waitingThreads.addAll(threadsWaitingForMyLocks(thread, locksInUse))));
            return waitingThreads;
        } catch (ConcurrentModificationException e) {
            // the GC may be cleaning up entries form the WeakHashMap. Nothing we can do about it. Let's assume there are no threads waiting. A new attempt will reveil issues.
            return Collections.emptySet();
        }
    }

    @Override
    public Lock obtainLock(String identifier) {
        boolean lockObtained = false;
        DisposableLock lock = null;
        while (!lockObtained) {
            lock = lockFor(identifier);
            lockObtained = lock.lock();
            if (!lockObtained) {
                log.warn("Failed to obtain lock for identifier({}), retrying...", identifier);
                locks.remove(identifier, lock);
            }
        }

        log.info("Obtained lock for identifier({})", identifier);

        return lock;
    }

    private DisposableLock lockFor(String identifier) {
        return locks.computeIfAbsent(identifier, DisposableLock::new);
    }

    private static final class PubliclyOwnedReentrantLock extends ReentrantLock {

        private static final long serialVersionUID = -2259228494514612163L;

        @Override
        public Collection<Thread> getQueuedThreads() { // NOSONAR
            return super.getQueuedThreads();
        }

        public boolean isHeldBy(Thread thread) {
            return thread.equals(getOwner());
        }
    }

    private class DisposableLock implements Lock {

        private final String identifier;
        private final PubliclyOwnedReentrantLock lock;
        private volatile boolean isClosed = false;

        private DisposableLock(String identifier) {
            this.identifier = identifier;
            this.lock = new PubliclyOwnedReentrantLock();
        }

        @Override
        public void release() {
            try {
                lock.unlock();
                log.debug("Released lock for identifier({})", identifier);
            } finally {
                disposeIfUnused();
            }
        }

        @Override
        public boolean isHeld() {
            return lock.isHeldByCurrentThread();
        }

        public boolean lock() {
            if (lock.getQueueLength() >= maximumQueued) {
                throw new LockAcquisitionFailedException("Failed to acquire lock for identifier " + identifier + ": too many queued threads.");
            }
            try {
                if (!lock.tryLock(0, TimeUnit.NANOSECONDS)) {
                    int attempts = acquireAttempts - 1;
                    do {
                        attempts--;
                        checkForDeadlock();
                        if (attempts < 1) {
                            throw new LockAcquisitionFailedException(
                                    "Failed to acquire lock for identifier(" + identifier + "), maximum attempts exceeded (" + acquireAttempts + ")"
                            );
                        }
                    } while (!lock.tryLock(lockAttemptTimeout, TimeUnit.MILLISECONDS));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LockAcquisitionFailedException("Thread was interrupted", e);
            }
            if (isClosed) {
                lock.unlock();
                return false;
            }
            return true;
        }

        private void checkForDeadlock() {
            if (!lock.isHeldByCurrentThread() && lock.isLocked()) {
                for (Thread thread : threadsWaitingForMyLocks(Thread.currentThread())) {
                    if (lock.isHeldBy(thread)) {
                        throw new DeadlockException(
                                "An imminent deadlock was detected while attempting to acquire a lock"
                        );
                    }
                }
            }
        }

        private void disposeIfUnused() {
            if (lock.tryLock()) {
                try {
                    if (lock.getHoldCount() == 1) {
                        // we now have a lock. We can shut it down.
                        isClosed = true;
                        locks.remove(identifier, this);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        public Collection<Thread> queuedThreads() {
            return lock.getQueuedThreads();
        }

        public boolean isHeldBy(Thread owner) {
            return lock.isHeldBy(owner);
        }
    }
}
