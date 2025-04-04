package com.kaua.event.driven.infrastructure.es.inspector;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.*;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class AnnotationHandlerInspectorTest extends UnitTest {

    @Test
    void givenAggregateModelRootEventHandlerSupportsReset_whenCallInspect_thenFindHandlers() {
        final var inspector = AnnotationHandlerInspector.inspect(AggregateModelRootEventHandlerSupportsReset.class);

        Assertions.assertEquals(5,
                inspector.getHandlers(AggregateModelRootEventHandlerSupportsReset.class).count());
    }

    @Test
    void givenAggregateModelRootEventHandlerSupportsReset_whenCallInspect_thenThrowException() {
        final var expectedErrorMessage = "java.lang.IllegalAccessException: no such method: com.kaua.event.driven.infrastructure.es.inspector.AnnotationHandlerInspectorTest$AggregateModelRootEventHandlerError.handle(EventTest)void/invokeVirtual";
        final var aException = Assertions.assertThrows(
                RuntimeException.class,
                () -> AnnotationHandlerInspector.inspect(AggregateModelRootEventHandlerError.class)
        );

        Assertions.assertEquals(expectedErrorMessage, aException.getMessage());
    }

    public static class AggregateModelRootEventHandlerError {

        @EventHandler
        private void handle(EventTest event) {
            System.out.println(event);
        }
    }


    public static class AggregateModelRootEventHandlerSupportsReset {

        @EventHandler
        public void handle(EventTest event) {
            System.out.println(event);
        }

        @EventHandler
        @AllowReplay(value = false)
        public void handle(StubDomainEvent event) {
            System.out.println(event);
        }

        @ResetHandler
        public void reset() {
            System.out.println("reset");
        }

        @EventSourcingHandler
        public void apply(EventTest event) {
            System.out.println(event);
        }

        @CommandHandler
        public void handle(CommandTest command) {
            System.out.println(command);
        }
    }

    public record CommandTest(
            String commandId,
            String commandType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String traceId
    ) implements Command {

        public CommandTest(String aggregateId) {
            this("commandId", "commandType", Instant.now(), aggregateId, 0, "traceId");
        }
    }

    public record EventTest(
            String eventId,
            String eventType,
            Instant occurredOn,
            String aggregateId,
            long aggregateVersion,
            String source,
            String traceId
    ) implements DomainEvent {

        public EventTest(String aggregateId) {
            this("eventId", "eventType", Instant.now(), aggregateId, 0, "source", "traceId");
        }
    }
}
