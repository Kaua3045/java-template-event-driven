package com.kaua.event.driven.infrastructure.es.aggregates;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.exceptions.NoHandlerForEventException;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateVersion;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.CommandHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventSourcingHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.TargetAggregateIdentifier;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.kaua.event.driven.infrastructure.es.aggregates.AggregateLifecycle.apply;

class AggregateModelTest extends UnitTest {

    @Test
    void givenAValidAggregateRoot_whenCallNewDefaultAggregateModel_thenShouldReturnAggregateModel() {
        final var aAggregateRoot = new AggregateModelRoot(
                "id",
                0
        );
        final var aAggregateModel = new DefaultAggregateModel<>(AggregateModelRoot.class);

        Assertions.assertNotNull(aAggregateModel);
        Assertions.assertEquals(1, aAggregateModel.commandHandlers().size());
        Assertions.assertEquals(1, aAggregateModel.eventHandlers().size());
        Assertions.assertEquals(0, aAggregateModel.commandHandlersInterceptors().size());
        Assertions.assertEquals(0, aAggregateModel.getAggregateVersion(aAggregateRoot));
        Assertions.assertEquals("id", aAggregateModel.getAggregateIdentifier(aAggregateRoot));
        Assertions.assertEquals(aAggregateRoot.getClass(), aAggregateModel.entityClass());
    }

    @Test
    void givenAnInvalidNullAggregateRoot_whenCallNewDefaultAggregateModel_thenThrowsException() {
        final var aException = Assertions.assertThrows(
                InternalErrorException.class,
                () -> new DefaultAggregateModel<>(null)
        );

        Assertions.assertEquals("Aggregate root class cannot be null", aException.getMessage());
    }

    @Test
    void givenAValidEvent_whenCallPublishInModel_thenHandleEvent() {
        final var aAggregateRoot = new AggregateModelRoot(
                "123",
                0
        );
        final var aAggregateModel = new DefaultAggregateModel<>(AggregateModelRoot.class);

        aAggregateModel.publish(aAggregateRoot, new EventTest("id"));

        Assertions.assertEquals("id", aAggregateRoot.getId());
        Assertions.assertEquals(0, aAggregateRoot.getVersion());
    }

    @Test
    void givenAnInvalidEvent_whenCallPublishInModel_thenThrowsException() {
        final var aAggregateRoot = new AggregateModelRoot(
                "123",
                0
        );
        final var aAggregateModel = new DefaultAggregateModel<>(AggregateModelRoot.class);

        final var aException = Assertions.assertThrows(
                NoHandlerForEventException.class,
                () -> aAggregateModel.publish(aAggregateRoot,
                        new StubDomainEvent(IdentifierUtils.generateNewId(), 0, "text"))
        );

        Assertions.assertEquals("No matching handler available to handle event [StubDomainEvent]", aException.getMessage());
    }

    public static class AggregateModelRoot {

        @TargetAggregateIdentifier
        private String id;

        @AggregateVersion
        private Long version;

        public AggregateModelRoot() {
        }

        public AggregateModelRoot(String id, long version) {
            this.id = id;
            this.version = version;
        }

        @CommandHandler
        public void handle(CommandTest command) {
            apply(new EventTest(command.aggregateId()));
        }

        @EventSourcingHandler
        public void on(EventTest event) {
            this.id = event.aggregateId();
            this.version = event.aggregateVersion();
        }

        public String getId() {
            return id;
        }

        public Long getVersion() {
            return version;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setVersion(Long version) {
            this.version = version;
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
