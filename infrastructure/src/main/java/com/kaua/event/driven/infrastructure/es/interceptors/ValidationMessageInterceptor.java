package com.kaua.event.driven.infrastructure.es.interceptors;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.validation.AssertionConcern;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiFunction;

public class ValidationMessageInterceptor<T>
        implements MessageHandlerInterceptor<T>, MessageDispatcherInterceptor<T>, AssertionConcern {

    private static final Logger log = LoggerFactory.getLogger(ValidationMessageInterceptor.class);

    @Override
    public Object handle(
            UnitOfWork<? extends T> unitOfWork,
            InterceptorChain interceptorChain
    ) throws Exception {
        handle(unitOfWork.getMessage());
        return interceptorChain.process();
    }

    @Nonnull
    @Override
    public BiFunction<Integer, T, T> handle(@Nonnull List<? extends T> messages) {
        return (index, message) -> {
            if (message instanceof Command command) {
                log.debug("Validating command: {}", command);
                assertArgumentNotEmpty(command.commandId(), "commandId", "should not be empty");
                assertArgumentNotEmpty(command.aggregateId(), "aggregateId", "should not be empty");
                assertArgumentGreaterOrEquals(0, (int) command.aggregateVersion(), "aggregateVersion", "should be greater or equals 0");
            }

            if (message instanceof DomainEvent event) {
                log.debug("Validating event: {}", event);
                assertArgumentNotEmpty(event.eventId(), "eventId", "should not be empty");
                assertArgumentNotEmpty(event.aggregateId(), "aggregateId", "should not be empty");
                assertArgumentGreaterOrEquals(0, (int) event.aggregateVersion(), "aggregateVersion", "should be greater or equals 0");
            }

            log.info("Message validated: {}", message);

            return message;
        };
    }
}
