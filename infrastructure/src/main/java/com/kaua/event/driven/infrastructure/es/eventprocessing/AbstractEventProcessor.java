package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.validation.AssertionConcern;
import com.kaua.event.driven.infrastructure.es.command.callback.MessageMonitor;
import com.kaua.event.driven.infrastructure.es.command.callback.NoOpMessageMonitor;
import com.kaua.event.driven.infrastructure.es.interceptors.DefaultInterceptorChain;
import com.kaua.event.driven.infrastructure.es.interceptors.MessageHandlerInterceptor;
import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import com.kaua.event.driven.infrastructure.uow.RollbackConfiguration;
import com.kaua.event.driven.infrastructure.uow.RollbackConfigurationType;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// TODO trace - span
public abstract class AbstractEventProcessor implements EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(AbstractEventProcessor.class);

    private final String name;
    private final EventHandlerInvoker eventHandlerInvoker;
    private final RollbackConfiguration rollbackConfiguration;
    private final List<MessageHandlerInterceptor<? super DomainEvent>> interceptors = new CopyOnWriteArrayList<>();
    private final MessageMonitor<? super DomainEvent> messageMonitor;

    public AbstractEventProcessor(Builder builder) {
        this.name = builder.name;
        this.eventHandlerInvoker = builder.eventHandlerInvoker;
        this.rollbackConfiguration = builder.rollbackConfiguration;
        this.messageMonitor = builder.messageMonitor;
    }

    @Override
    public String getName() {
        return name;
    }

    public void registerHandlerInterceptor(MessageHandlerInterceptor<? super DomainEvent> interceptor) {
        interceptors.add(interceptor);
    }

    protected boolean canHandle(DomainEvent event) {
        return eventHandlerInvoker.canHandle(event);
    }

    protected final void processInUnitOfWork(List<? extends DomainEvent> events, UnitOfWork<? extends DomainEvent> unitOfWork) throws Exception {
        ResultMessage<?> resultMessage = unitOfWork.executeWithResult(() -> {
            DomainEvent event = unitOfWork.getMessage();
            MessageMonitor.MonitorCallback monitorCallback = messageMonitor.onMessageIngested(event);
            return new DefaultInterceptorChain<>(
                    m -> processMessage(event, monitorCallback),
                    interceptors,
                    unitOfWork
            ).process();
        }, rollbackConfiguration);

        if (resultMessage.isExceptional()) {
            Throwable e = resultMessage.getExceptionResult();
            if (unitOfWork.isRolledBack()) {
                log.debug("Unit of work rolled back. Ignoring exception");
            } else {
                log.warn(
                        "Exception occurred while processing a message, but unit of work was committed. {}",
                        e.getClass().getName());
            }
        }
    }

    protected Object processMessage(DomainEvent event, MessageMonitor.MonitorCallback monitorCallback) {
        try {
            log.debug("Processing event before invoke: {}", event);
            eventHandlerInvoker.handle(event);
            monitorCallback.reportSuccess();
            return null;
        } catch (Exception e) {
            monitorCallback.reportFailure(e);
            throw new RuntimeException(e);
        }
    }

    public EventHandlerInvoker eventHandlerInvoker() {
        return eventHandlerInvoker;
    }

    public static abstract class Builder implements AssertionConcern {
        private String name;
        private EventHandlerInvoker eventHandlerInvoker;
        private RollbackConfiguration rollbackConfiguration = RollbackConfigurationType.ANY_THROWABLE;
        private MessageMonitor<? super DomainEvent> messageMonitor = NoOpMessageMonitor.INSTANCE;

        public Builder name(String name) {
            assertArgumentNotEmpty(name, "name", "should not be empty because it is mandatory");
            this.name = name;
            return this;
        }

        public Builder eventHandlerInvoker(EventHandlerInvoker eventHandlerInvoker) {
            assertArgumentNotNull(eventHandlerInvoker, "eventHandlerInvoker", "should not be null because it is mandatory");
            this.eventHandlerInvoker = eventHandlerInvoker;
            return this;
        }

        public Builder rollbackConfiguration(RollbackConfiguration rollbackConfiguration) {
            this.rollbackConfiguration = rollbackConfiguration;
            return this;
        }

        public Builder messageMonitor(MessageMonitor<? super DomainEvent> messageMonitor) {
            this.messageMonitor = messageMonitor;
            return this;
        }
    }
}
