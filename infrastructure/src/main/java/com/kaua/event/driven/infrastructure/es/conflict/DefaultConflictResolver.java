package com.kaua.event.driven.infrastructure.es.conflict;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.ConflictAggregateVersionException;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;

public class DefaultConflictResolver implements ConflictResolver {

    private static final Logger log = LoggerFactory.getLogger(DefaultConflictResolver.class);

    private final EventStore eventStore;
    private final String aggregateId;
    private final long expectedVersion;
    private final long actualVersion;
    private List<DomainEvent> conflictingEvents;
    private boolean conflictsResolved = false;

    public DefaultConflictResolver(
            EventStore eventStore,
            String aggregateId,
            long expectedVersion,
            long actualVersion
    ) {
        this.eventStore = eventStore;
        this.aggregateId = aggregateId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    @Override
    public void ensureConflictsResolved() {
        if (!conflictsResolved) {
            log.error("Conflicts not resolved for aggregate {}", aggregateId);
            throw new ConflictAggregateVersionException(aggregateId, expectedVersion, actualVersion);
        }
        log.info("Conflicts resolved for aggregate {}", aggregateId);
    }

    @Override
    public <T extends Exception> void detectConflicts(Predicate<List<DomainEvent>> predicate, ConflictExceptionSupplier<T> exceptionSupplier) throws T {
        conflictsResolved = true;
        List<DomainEvent> unexpectedEvents = loadConflictingEvents();
        if (predicate.test(unexpectedEvents)) {
            log.debug("Conflicts detected for aggregate {}", aggregateId);
            T exception = exceptionSupplier.supplyException(
                    new ConflictAggregateVersionException(aggregateId, expectedVersion, actualVersion)
            );

            if (exception != null) {
                log.error("Conflict detected for aggregate {}, exception {}", aggregateId, exception.getMessage());
                throw exception;
            }
        }

        log.info("No conflicts detected for aggregate {}", aggregateId);
    }

    private List<DomainEvent> loadConflictingEvents() {
        if (conflictingEvents == null) {
            if (expectedVersion >= actualVersion) {
                return List.of();
            }

            conflictingEvents = this.eventStore.readEvents(aggregateId)
                    .stream() // check event version is greater than expected version and less than or equal to actual version
                    .filter(event -> event.aggregateVersion() > expectedVersion
                            && event.aggregateVersion() <= actualVersion)
                    .toList(); // and return conflicting events

        }
        return conflictingEvents;
    }
}
