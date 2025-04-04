package com.kaua.event.driven.infrastructure.es.eventprocessing.messagesource;

import com.kaua.event.driven.domain.UnitTest;
import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.utils.IdentifierUtils;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;
import com.kaua.event.driven.utils.StubDomainEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

class KafkaMessageSourceTest extends UnitTest {

    private KafkaMessageSource kafkaMessageSource;

    private final KafkaConsumer<String, String> kafkaConsumer = mock(KafkaConsumer.class);

    @BeforeEach
    void setup() {
        kafkaMessageSource = KafkaMessageSource.builder()
                .name("test")
                .topics(List.of("test-topic"))
                .consumer(kafkaConsumer)
                .build();
    }

    @Test
    void givenAValidReplayTrackingToken_whenCallReadEvents_thenReturnEventsWithContinueReplay() {
        final var aCurrentToken = createKafkaTrackingToken(1L);
        final var aPointOfStartResetToken = createKafkaTrackingToken(0L);
        final var aReplayToken = ReplayToken.createReplayToken(
                aCurrentToken,
                aPointOfStartResetToken,
                "test"
        );

        final var aEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        Mockito.when(kafkaConsumer.listTopics()).thenReturn(Map.of("test-topic", List.of(new PartitionInfo("test-topic", 0, null, null, null))));
        Mockito.doNothing().when(kafkaConsumer).assign(any());
        Mockito.doNothing().when(kafkaConsumer).seek(
                aPointOfStartResetToken.getPositions().entrySet().stream().findFirst().get().getKey(),
                aPointOfStartResetToken.getOffset()
        );
        Mockito.when(kafkaConsumer.poll(any())).thenReturn(new ConsumerRecords<>(
                Map.of(aPointOfStartResetToken.getPositions().entrySet().stream().findFirst().get().getKey(),
                        List.of(createConsumerRecord(aEvent, 0), createConsumerRecord(aEvent, 1)))
        ));

        final var events = kafkaMessageSource.readEvents(aReplayToken);

        Assertions.assertNotNull(events);
        Assertions.assertEquals(2, events.size());

        final var aFirstEventResult = events.getFirst();
        Assertions.assertTrue(validateEvent(aEvent, aFirstEventResult));

        if (aFirstEventResult.trackingToken() instanceof ReplayToken replayToken) {
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getTokenAtReset());
            validateKafkaTrackingToken(aPointOfStartResetToken, (KafkaTrackingToken) replayToken.getCurrentToken());
        }

        final var aSecondEventResult = events.getLast();
        Assertions.assertTrue(validateEvent(aEvent, aSecondEventResult));

        if (aSecondEventResult.trackingToken() instanceof ReplayToken replayToken) { // it arrived in the (currentToken) on replay is started
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getTokenAtReset());
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getCurrentToken());
        }
    }

    @Test
    void givenAValidReplayTokenAndNewEventToProcess_whenCallReadEvents_thenProcessEventsAndReturnIsNotReplayInLastEvent() {
        final var aCurrentToken = createKafkaTrackingToken(1L);
        final var aPointOfStartResetToken = createKafkaTrackingToken(0L);
        final var aReplayToken = ReplayToken.createReplayToken(
                aCurrentToken,
                aPointOfStartResetToken,
                "test"
        );

        final var aEvent = new StubDomainEvent(IdentifierUtils.generateNewId(), "test");

        Mockito.when(kafkaConsumer.listTopics()).thenReturn(Map.of("test-topic", List.of(new PartitionInfo("test-topic", 0, null, null, null))));
        Mockito.doNothing().when(kafkaConsumer).assign(any());
        Mockito.doNothing().when(kafkaConsumer).seek(
                aPointOfStartResetToken.getPositions().entrySet().stream().findFirst().get().getKey(),
                aPointOfStartResetToken.getOffset()
        );
        Mockito.when(kafkaConsumer.poll(any())).thenReturn(new ConsumerRecords<>(
                Map.of(aPointOfStartResetToken.getPositions().entrySet().stream().findFirst().get().getKey(),
                        List.of(createConsumerRecord(aEvent, 0), createConsumerRecord(aEvent, 1), createConsumerRecord(aEvent, 2)))
        ));

        final var events = kafkaMessageSource.readEvents(aReplayToken);

        Assertions.assertNotNull(events);
        Assertions.assertEquals(3, events.size());

        final var aFirstEventResult = events.getFirst();
        Assertions.assertTrue(validateEvent(aEvent, aFirstEventResult));

        if (aFirstEventResult.trackingToken() instanceof ReplayToken replayToken) {
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getTokenAtReset());
            validateKafkaTrackingToken(aPointOfStartResetToken, (KafkaTrackingToken) replayToken.getCurrentToken());
        }

        final var aSecondEventResult = events.get(1);
        Assertions.assertTrue(validateEvent(aEvent, aSecondEventResult));

        if (aSecondEventResult.trackingToken() instanceof ReplayToken replayToken) { // it arrived in the (currentToken) on replay is started
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getTokenAtReset());
            validateKafkaTrackingToken(aCurrentToken, (KafkaTrackingToken) replayToken.getCurrentToken());
        }

        final var aLastEvent = events.getLast();
        Assertions.assertTrue(validateEvent(aEvent, aLastEvent));

        if (aLastEvent.trackingToken() instanceof KafkaTrackingToken kafkaTrackingToken) {
            Assertions.assertEquals(aCurrentToken.getProcessorName(), kafkaTrackingToken.getProcessorName());
            Assertions.assertEquals(2L, kafkaTrackingToken.getOffset());
            kafkaTrackingToken.getPositions().forEach((key, value) -> {
                Assertions.assertEquals(2L, value);
                Assertions.assertEquals(aCurrentToken.getPositions().entrySet().stream().findFirst().get().getKey(), key);
            });
        } else {
            throw new RuntimeException("Tracking token is not instance of KafkaTrackingToken");
        }
    }

    @Test
    void testCreateDefaultTrackingToken() {
        final var aTrackingToken = kafkaMessageSource.defaultTrackingToken();

        Assertions.assertNotNull(aTrackingToken);
        Assertions.assertInstanceOf(KafkaTrackingToken.class, aTrackingToken);
        Assertions.assertEquals("test", aTrackingToken.processorName());
        Assertions.assertEquals(0L, ((KafkaTrackingToken) aTrackingToken).getOffset());
        Assertions.assertTrue(((KafkaTrackingToken) aTrackingToken).getPositions().isEmpty());
    }

    private KafkaTrackingToken createKafkaTrackingToken(long offset) {
        return new KafkaTrackingToken(
                "test",
                Instant.now(),
                offset,
                Map.of(new TopicPartition("test-topic", 0), offset)
        );
    }

    private ConsumerRecord<String, String> createConsumerRecord(StubDomainEvent aEvent, long offset) {
        final var aRecord = new ConsumerRecord<>(
                "test-topic",
                0,
                offset,
                aEvent.aggregateId(),
                Json.writeValueAsString(aEvent)
        );

        aRecord.headers().add("event_type", aEvent.eventType().getBytes());

        return aRecord;
    }

    private boolean validateEvent(DomainEvent expected, DomainEvent actual) {
        return expected.eventId().equals(actual.eventId()) &&
                expected.aggregateId().equals(actual.aggregateId()) &&
                expected.occurredOn().equals(actual.occurredOn()) &&
                expected.eventType().equals(actual.eventType()) &&
                expected.aggregateVersion() == actual.aggregateVersion();
    }

    private void validateKafkaTrackingToken(KafkaTrackingToken expected, KafkaTrackingToken actual) {
        Assertions.assertEquals(expected.getProcessorName(), actual.getProcessorName());
        Assertions.assertEquals(expected.getOffset(), actual.getOffset());
        Assertions.assertTrue(expected.getPositions().entrySet().containsAll(actual.getPositions().entrySet()));
    }
}
