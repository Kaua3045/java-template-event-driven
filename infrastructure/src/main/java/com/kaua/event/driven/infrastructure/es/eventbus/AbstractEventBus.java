package com.kaua.event.driven.infrastructure.es.eventbus;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.domain.exceptions.NotFoundException;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.uow.CurrentUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import static com.kaua.event.driven.infrastructure.uow.UnitOfWork.Phase.*;

//  TODO need attention
public abstract class AbstractEventBus implements EventBus {

    private static final Logger log = LoggerFactory.getLogger(AbstractEventBus.class);

    private final String eventsKey = this + "_EVENTS";
    private final Set<Consumer<List<? extends DomainEvent>>> eventProcessors = new CopyOnWriteArraySet<>();

    protected AbstractEventBus() {
    }

    @Override
    public void publish(DomainEvent event) {
        handle(event);
    }

    public void handle(DomainEvent event) {
        if (CurrentUnitOfWork.isStarted()) {
            UnitOfWork<?> uow = CurrentUnitOfWork.get();
            Assert.state(!uow.phase().isAfter(PREPARE_COMMIT),
                    () -> "It is not allowed to publish events when the current Unit of Work has already been " +
                            "committed. Please start a new Unit of Work before publishing events.");
            Assert.state(!uow.root().phase().isAfter(PREPARE_COMMIT),
                    () -> "It is not allowed to publish events when the root Unit of Work has already been " +
                            "committed.");

            log.debug("Publishing event {} for other handlers", event);
            eventsQueue(uow).add(event);
        } else {
            log.debug("Preparing to publish event {} for other handlers", event);
            final var events = new ArrayList<DomainEvent>();
            events.add(event);
            prepareCommit(events);
            commit(events);
            afterCommit(events);
            log.info("Event {} published", event);
        }
    }

    private List<DomainEvent> eventsQueue(UnitOfWork<?> unitOfWork) {
        return unitOfWork.getOrComputeResource(eventsKey, r -> {
            List<DomainEvent> eventQueue = new ArrayList<>();

            unitOfWork.onPrepareCommit(u -> {
                if (u.parent().isPresent() && !u.parent().get().phase().isAfter(PREPARE_COMMIT)) {
                    eventsQueue(u.parent().get()).addAll(eventQueue);
                } else {
                    int processedItems = eventQueue.size();
                    doWithEvents(this::prepareCommit, eventQueue);
//                    doWithEvents(this::prepareCommit, intercept(eventQueue));
                    // Make sure events published during publication prepare commit phase are also published
                    while (processedItems < eventQueue.size()) {
                        List<? extends DomainEvent> newMessages = eventQueue.subList(processedItems, eventQueue.size());
//                                intercept(eventQueue.subList(processedItems, eventQueue.size()));
                        processedItems = eventQueue.size();
                        doWithEvents(this::prepareCommit, newMessages);
                    }
                }
            });
            unitOfWork.onCommit(u -> {
                if (u.parent().isPresent() && !u.root().phase().isAfter(COMMIT)) {
                    u.root().onCommit(w -> doWithEvents(this::commit, eventQueue));
                } else {
                    doWithEvents(this::commit, eventQueue);
                }
            });
            unitOfWork.afterCommit(u -> {
                if (u.parent().isPresent() && !u.root().phase().isAfter(AFTER_COMMIT)) {
                    u.root().afterCommit(w -> doWithEvents(this::afterCommit, eventQueue));
                } else {
                    doWithEvents(this::afterCommit, eventQueue);
                }
            });
            unitOfWork.onCleanup(u -> {
                u.resources().remove(eventsKey);
            });
            return eventQueue;
        });
    }

    protected List<DomainEvent> queuedMessages() {
        if (!CurrentUnitOfWork.isStarted()) {
            return Collections.emptyList();
        }
        List<DomainEvent> messages = new ArrayList<>();
        addStagedMessages(CurrentUnitOfWork.get(), messages);
        return messages;
    }

    private void addStagedMessages(UnitOfWork<?> unitOfWork, List<DomainEvent> messages) {
        unitOfWork.parent().ifPresent(parent -> addStagedMessages(parent, messages));
        if (unitOfWork.isRolledBack()) {
            // staged messages are irrelevant if the UoW has been rolled back
            return;
        }
        List<DomainEvent> stagedEvents = unitOfWork.getOrDefaultResource(eventsKey, Collections.emptyList());
        for (DomainEvent stagedEvent : stagedEvents) {
            if (!messages.contains(stagedEvent)) {
                messages.add(stagedEvent);
            }
        }
    }

//    protected List<? extends DomainEvent> intercept(List<? extends DomainEvent> events) {
//        List<DomainEvent> preprocessedEvents = new ArrayList<>(events);
//        for (MessageDispatchInterceptor<? super DomainEvent> preprocessor : dispatchInterceptors) {
//            BiFunction<Integer, ? super DomainEvent, ? super DomainEvent> function =
//                    preprocessor.handle(preprocessedEvents);
//            for (int i = 0; i < preprocessedEvents.size(); i++) {
//                preprocessedEvents.set(i, (DomainEvent) function.apply(i, preprocessedEvents.get(i)));
//            }
//        }
//        return preprocessedEvents;
//    }

    private void doWithEvents(Consumer<List<? extends DomainEvent>> eventsConsumer,
                              List<? extends DomainEvent> events) {
        eventsConsumer.accept(events);
    }

    protected void prepareCommit(List<? extends DomainEvent> events) {
        eventProcessors.forEach(eventProcessor -> eventProcessor.accept(events));
    }

    protected void commit(List<? extends DomainEvent> events) {
    }

    protected void afterCommit(List<? extends DomainEvent> events) {
    }
}
