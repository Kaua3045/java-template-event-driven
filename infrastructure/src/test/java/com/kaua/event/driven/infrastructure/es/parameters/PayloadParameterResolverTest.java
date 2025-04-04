package com.kaua.event.driven.infrastructure.es.parameters;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class PayloadParameterResolverTest extends UnitTest {

    @Test
    void givenAValidTrackedEventMessage_whenCallResolve_thenShouldReturnEvent() {
        final var aEvent = new EventTest(IdentifierUtils.generateNewId());
        final var aTrackedEventMessage = createTrackedEventMessage(aEvent);

        final var aPayloadResolver = new PayloadParameterResolver(EventTest.class);

        final var aResolved = aPayloadResolver.resolve(aTrackedEventMessage);

        Assertions.assertEquals(aEvent, aResolved);
        Assertions.assertTrue(aPayloadResolver.matches(aEvent));
    }

    @Test
    void givenAValidDomainEvent_whenCallResolve_thenShouldReturnEvent() {
        final var aEvent = new EventTest(IdentifierUtils.generateNewId());

        final var aPayloadResolver = new PayloadParameterResolver(EventTest.class);

        final var aResolved = aPayloadResolver.resolve(aEvent);

        Assertions.assertEquals(aEvent, aResolved);
        Assertions.assertTrue(aPayloadResolver.matches(aEvent));
    }

    @Test
    void givenAValidCommand_whenCallResolve_thenShouldReturnCommand() {
        final var aCommand = new CommandTest(IdentifierUtils.generateNewId());

        final var aPayloadResolver = new PayloadParameterResolver(CommandTest.class);

        final var aResolved = aPayloadResolver.resolve(aCommand);

        Assertions.assertEquals(aCommand, aResolved);
        Assertions.assertTrue(aPayloadResolver.matches(aCommand));
    }

    @Test
    void givenAnInvalidObject_whenCallResolve_thenThrowsException() {
        final var aObject = new Object();
        final var expectedErrorMessage = "Unsupported payload type: class java.lang.Object";

        final var aPayloadResolver = new PayloadParameterResolver(EventTest.class);

        final var aException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> aPayloadResolver.resolve(aObject)
        );

        Assertions.assertEquals(expectedErrorMessage, aException.getMessage());
        Assertions.assertFalse(aPayloadResolver.matches(aObject));
    }

    @Test
    void givenAnInvalidNull_whenCallMatches_thenShouldReturnFalse() {
        final var aPayloadResolver = new PayloadParameterResolver(EventTest.class);

        final var aResolved = aPayloadResolver.matches(null);

        Assertions.assertFalse(aResolved);
    }

    private TrackedEventMessage<?> createTrackedEventMessage(DomainEvent aEvent) {
        return new TrackedEventMessage<>() {
            @Override
            public String eventId() {
                return aEvent.eventId();
            }

            @Override
            public String eventType() {
                return aEvent.eventType();
            }

            @Override
            public Instant occurredOn() {
                return aEvent.occurredOn();
            }

            @Override
            public String aggregateId() {
                return aEvent.aggregateId();
            }

            @Override
            public long aggregateVersion() {
                return aEvent.aggregateVersion();
            }

            @Override
            public String source() {
                return aEvent.source();
            }

            @Override
            public String traceId() {
                return aEvent.traceId();
            }

            @Override
            public DomainEvent getPayload() {
                return aEvent;
            }

            @Override
            public TrackingToken trackingToken() {
                return new TrackingTokenTest();
            }
        };
    }

    public class TrackingTokenTest implements TrackingToken {

        @Override
        public String processorName() {
            return "teste";
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
