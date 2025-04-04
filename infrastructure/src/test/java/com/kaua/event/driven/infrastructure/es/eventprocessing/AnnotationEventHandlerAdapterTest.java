package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.domain.utils.InstantUtils;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AllowReplay;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.EventHandler;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.ResetHandler;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

class AnnotationEventHandlerAdapterTest extends UnitTest {

    @Test
    void givenAValidDomainEvent_whenCallCanHandle_thenReturnTrue() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        boolean canHandle = annotationEventHandlerAdapter.canHandle(new EventTest("aggregateId"));

        Assertions.assertTrue(canHandle);
    }

    @Test
    void givenAValidTrackedDomainEvent_whenCallCanHandle_thenReturnTrue() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        final var aEvent = new EventTest("aggregateId");
        boolean canHandle = annotationEventHandlerAdapter
                .canHandle(new TrackedEventMessage<>() {
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
                        return () -> "teste";
                    }
                });

        Assertions.assertTrue(canHandle);
    }

    @Test
    void givenAnNullMessage_whenCallCanHandle_thenReturnTrue() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        boolean canHandle = annotationEventHandlerAdapter.canHandle(null);

        Assertions.assertTrue(canHandle);
    }

    @Test
    void givenAnNotExpectedMessage_whenCallCanHandle_thenReturnFalse() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        boolean canHandle = annotationEventHandlerAdapter.canHandle(new DomainEvent() {
            @Override
            public String eventId() {
                return IdentifierUtils.generateNewId();
            }

            @Override
            public String eventType() {
                return "testes";
            }

            @Override
            public Instant occurredOn() {
                return InstantUtils.now();
            }

            @Override
            public String aggregateId() {
                return IdentifierUtils.generateNewId();
            }

            @Override
            public long aggregateVersion() {
                return 0;
            }

            @Override
            public String source() {
                return "source";
            }

            @Override
            public String traceId() {
                return "trace";
            }
        });

        Assertions.assertFalse(canHandle);
    }

    @Test
    void givenAValidDomainEvent_whenCallHandle_thenCallHandler() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        try {
            final var aResult = annotationEventHandlerAdapter.handle(new EventTest("aggregateId"));
            Assertions.assertNull(aResult);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void givenAValidTrackedDomainEvent_whenCallHandle_thenCallHandler() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        final var aEvent = new EventTest("aggregateId");
        try {
            final var aResult = annotationEventHandlerAdapter
                    .handle(new TrackedEventMessage<>() {
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
                            return () -> "teste";
                        }
                    });
            Assertions.assertNull(aResult);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void givenAValidTrackedDomainEvent_whenCallHandleButAllowReplayFalse_thenReturnNull() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        final var aEvent = new EventTest("aggregateId");
        try {
            final var aResult = annotationEventHandlerAdapter
                    .handle(new TrackedEventMessage<>() {
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
                            return new ReplayToken(
                                    new KafkaTrackingToken(
                                            "teste",
                                            aEvent.occurredOn(),
                                            0L,
                                            Map.of()
                                    ),
                                    new KafkaTrackingToken(
                                            "teste",
                                            aEvent.occurredOn(),
                                            0L,
                                            Map.of()
                                    ),
                                    "teste"
                            );
                        }
                    });
            Assertions.assertNull(aResult);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void givenAValidClassWithSupportsResetAnnotation_whenCallSupportsReset_thenReturnTrue() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandlerSupportsReset()
        );

        boolean supportsReset = annotationEventHandlerAdapter.supportsReset();

        Assertions.assertTrue(supportsReset);
    }

    @Test
    void givenAnInvalidClassWithSupportsResetAnnotation_whenCallSupportsReset_thenReturnFalse() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandler()
        );

        boolean supportsReset = annotationEventHandlerAdapter.supportsReset();

        Assertions.assertFalse(supportsReset);
    }

    @Test
    void givenAValidClassWithReset_whenCallPrepareReset_thenCallMethod() {
        AnnotationEventHandlerAdapter annotationEventHandlerAdapter = new AnnotationEventHandlerAdapter(
                new AggregateModelRootEventHandlerSupportsReset()
        );

        try {
            Assertions.assertDoesNotThrow(annotationEventHandlerAdapter::prepareReset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class AggregateModelRootEventHandler {

        @EventHandler
        public void handle(EventTest event) {
            System.out.println(event);
        }

        @EventHandler
        @AllowReplay(value = false)
        public void handle(StubDomainEvent event) {
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
