package com.kaua.event.driven.infrastructure.es.aggregates.repositories;

import com.kaua.event.driven.domain.exceptions.AggregateDeletionException;
import com.kaua.event.driven.domain.exceptions.ConflictAggregateVersionException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.Aggregate;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateRepository;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractRepository<T, A extends Aggregate<T>> implements AggregateRepository<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractRepository.class);

    private final AggregateModel<T> aggregateModel;

    public AbstractRepository(AggregateModel<T> aggregateModel) {
        this.aggregateModel = aggregateModel;
    }

    @Override
    public Aggregate<T> load(String aggregateIdentifier) {
        log.debug("Loading aggregate: {}", aggregateIdentifier);
        UnitOfWork<?> uow = currentUnitOfWork();
        Map<String, A> aggregates = managedAggregates(uow);
        A aggregate = aggregates.computeIfAbsent(
                aggregateIdentifier, s -> {
                    try {
                        return doLoad(s);
                    } catch (Exception e) {
                        log.error("Error loading aggregate", e);
                        throw e;
                    }
                }
        );
        uow.onRollback(u -> aggregates.remove(aggregateIdentifier));
        prepareForCommit(aggregate);

        log.info("Aggregate loaded: {}", aggregate.identifierAsString());

        return aggregate;
    }

    @Override
    public Aggregate<T> load(String aggregateIdentifier, Long expectedVersion) {
        log.debug("Loading aggregate with version, identifier: {}, version: {}", aggregateIdentifier, expectedVersion);
        UnitOfWork<?> uow = currentUnitOfWork();
        Map<String, A> aggregates = managedAggregates(uow);
        A aggregate = aggregates.computeIfAbsent(
                aggregateIdentifier, s -> {
                    try {
                        return doLoad(s, expectedVersion);
                    } catch (Exception e) {
                        log.error("Error loading aggregate", e);
                        throw e;
                    }
                }
        );
        uow.onRollback(u -> aggregates.remove(aggregateIdentifier));
        validateOnLoad(aggregate, expectedVersion);
        prepareForCommit(aggregate);

        log.info("Aggregate loaded with version and identifier, identifier: {}, version: {}", aggregate.identifierAsString(), expectedVersion);

        return aggregate;
    }

    @Override
    public Aggregate<T> newInstance(Callable<T> factoryMethod) throws Exception {
        log.debug("Creating new aggregate");
        UnitOfWork<?> uow = currentUnitOfWork();
        AtomicReference<A> aggregateRef = new AtomicReference<>();

        uow.onPrepareCommit(x -> {
            A aggregate = aggregateRef.get();

            if (aggregate != null && aggregate.identifier() != null) {
                prepareForCommit(aggregate);
            }
        });

        A aggregate;
        try {
            aggregate = doCreateNew(factoryMethod);
        } catch (Exception e) {
            log.error("Error creating new aggregate", e);
            throw e;
        }

        aggregateRef.set(aggregate);

        Map<String, A> aggregates = managedAggregates(uow);
        Assert.isTrue(aggregates.putIfAbsent(aggregate.identifierAsString(), aggregate) == null,
                () -> "The unit of work already has an aggregate with the same identifier: " + aggregate.identifierAsString());
        uow.onRollback(u -> aggregates.remove(aggregate.identifierAsString()));

        log.info("New aggregate created: {}", aggregate.identifierAsString());

        return aggregate;
    }

    @Override
    public Aggregate<T> loadOrCreate(String aggregateIdentifier, Callable<T> factoryMethod) {
        log.debug("Loading or creating aggregate: {}", aggregateIdentifier);
        UnitOfWork<?> uow = currentUnitOfWork();
        Map<String, A> aggregates = managedAggregates(uow);
        A aggregate = aggregates.computeIfAbsent(
                aggregateIdentifier,
                s -> {
                    try {
                        return doLoadOrCreate(aggregateIdentifier, factoryMethod);
                    } catch (AggregateDeletionException e) {
                        throw new AggregateDeletionException(aggregateIdentifier);
                    } catch (NotFoundException e) {
                        throw NotFoundException.withIdentifier(aggregateIdentifier);
                    } catch (Exception e) {
                        log.error("Error loading or creating aggregate", e);
                        throw new RuntimeException(e);
                    }
                }
        );
        uow.onRollback(u -> aggregates.remove(aggregateIdentifier));
        prepareForCommit(aggregate);

        // TODO identifier and version returning null
        log.info("Aggregate loaded or created with identifier: {} and version: {}", aggregate.identifierAsString(), aggregate.version());

        return aggregate;
    }

    protected void validateOnLoad(Aggregate<T> aggregate, Long expectedVersion) {
        if (expectedVersion != null && aggregate.version() != null && !expectedVersion.equals(aggregate.version())) {
            throw new ConflictAggregateVersionException(
                    aggregate.identifierAsString(),
                    expectedVersion,
                    aggregate.version()
            );
        }
    }

    private UnitOfWork<?> currentUnitOfWork() {
        return CurrentUnitOfWork.get();
    }

    protected Map<String, A> managedAggregates(UnitOfWork<?> uow) {
        return uow.root().getOrComputeResource("aggregates", k -> new HashMap<>());
    }

    protected void prepareForCommit(A aggregate) {
        if (UnitOfWork.Phase.STARTED.isBefore(CurrentUnitOfWork.get().phase())) {
            doCommit(aggregate);
        } else {
            CurrentUnitOfWork.get().onPrepareCommit(u -> doCommit(aggregate));
        }
    }

    private void doCommit(A aggregate) {
        if (managedAggregates(CurrentUnitOfWork.get()).containsValue(aggregate)) {
            if (aggregate.isDeleted()) {
                log.debug("Deleting aggregate: {}", aggregate.identifierAsString());
                doDelete(aggregate);
            } else {
                log.debug("Saving aggregate: {}", aggregate.identifierAsString());
                doSave(aggregate);
            }
        }
//            if (aggregate.isDeleted()) {
//                log.debug("Post delete aggregate: {}", aggregate.identifierAsString());
//            } else {
//                log.debug("Post save aggregate: {}", aggregate.identifierAsString());
//            }
//        } else {
//            log.debug("Aggregate not managed: {}", aggregate.identifierAsString());
//        }
    }

    protected abstract A doLoadOrCreate(String aggregateIdentifier, Callable<T> factoryMethod) throws Exception;

    protected abstract A doCreateNew(Callable<T> factoryMethod) throws Exception;

    protected abstract A doLoad(String aggregateIdentifier);

    protected abstract A doLoad(String aggregateIdentifier, Long expectedVersion);

    protected abstract void doSave(A aggregate);

    protected abstract void doDelete(A aggregate);

    public AggregateModel<T> getAggregateModel() {
        return aggregateModel;
    }
}
