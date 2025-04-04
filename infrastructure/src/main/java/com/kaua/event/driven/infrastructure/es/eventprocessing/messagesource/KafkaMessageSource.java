package com.kaua.event.driven.infrastructure.es.eventprocessing.messagesource;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.domain.exceptions.InternalErrorException;
import com.kaua.event.driven.domain.utils.InstantUtils;
import com.kaua.event.driven.domain.validation.AssertionConcern;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventprocessing.MessageSource;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

public class KafkaMessageSource implements MessageSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageSource.class);

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final String name;

    public static Builder builder() {
        return new Builder();
    }

    public KafkaMessageSource(Builder builder) {
        this.consumer = builder.consumer;
        this.topics = builder.topics;
        this.name = builder.name;
    }

    public String getName() {
        return name;
    }

    @Override
    public List<TrackedEventMessage<DomainEvent>> readEvents(TrackingToken trackingToken) {
        log.info("Reading events from Kafka for tracking token {}", trackingToken);

        TrackingToken finalTrackingToken = (trackingToken instanceof ReplayToken replayToken)
                ? replayToken.getCurrentToken()
                : trackingToken;

        seekToCurrentPositions(consumer, () -> (KafkaTrackingToken) finalTrackingToken, topics);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(6));

        if (records.isEmpty()) {
            log.warn("No records found for tracking token {}", trackingToken);
            return List.of();
        }

        List<TrackedEventMessage<DomainEvent>> events = new ArrayList<>();
        records.spliterator()
                .forEachRemaining(it -> {
                    try {
                        log.debug("Reading event from record: {}", it);
                        final var aClazz = Class.forName(new String(it.headers().lastHeader("event_type").value()));
                        final var aEvent = (DomainEvent) Json.readValue(it.value(), aClazz);

                        TrackingToken finalEventTrackingToken = createTrackingToken(trackingToken, it, aEvent); // its important to use de tracking token received in the method

                        events.add(createTrackedEvent(aEvent, finalEventTrackingToken));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        events.sort(Comparator.comparingLong(event -> {
            if (event.trackingToken() instanceof KafkaTrackingToken) {
                return ((KafkaTrackingToken) event.trackingToken()).getOffset();
            } else if (event.trackingToken() instanceof ReplayToken replayToken) {
                return ((KafkaTrackingToken) replayToken.getCurrentToken()).getOffset();
            }
            throw new InternalErrorException("Invalid tracking token type");
        }));

        return events;
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void commit() {
        consumer.commitSync();
    }

    @Override
    public TrackingToken defaultTrackingToken() {
        return new KafkaTrackingToken(getName(), InstantUtils.now(), 0L, new HashMap<>());
    }

    private TrackingToken createTrackingToken(TrackingToken actualTrackingToken, ConsumerRecord<String, String> record, DomainEvent domainEvent) {
        if (actualTrackingToken instanceof ReplayToken replayToken) {
            final var aOldKafkaToken = (KafkaTrackingToken) replayToken.getTokenAtReset();

            if (record.offset() > aOldKafkaToken.getOffset()) { // check if is not a replay event
                System.out.println("is not a replay event");
                return new KafkaTrackingToken(
                        getName(),
                        domainEvent.occurredOn(),
                        record.offset(),
                        Map.of(new TopicPartition(record.topic(), record.partition()), record.offset())
                );
            }

            // on is not ended replay
            return new ReplayToken(
                    replayToken.getTokenAtReset(),
                    new KafkaTrackingToken(
                            getName(),
                            domainEvent.occurredOn(),
                            record.offset(),
                            Map.of(new TopicPartition(record.topic(), record.partition()), record.offset())
                    ),
                    getName()
            );
        }

        // on is not a replay
        return new KafkaTrackingToken(
                getName(),
                domainEvent.occurredOn(),
                record.offset(),
                Map.of(new TopicPartition(record.topic(), record.partition()), record.offset())
        );
    }

    private TrackedEventMessage<DomainEvent> createTrackedEvent(DomainEvent aEvent, TrackingToken trackingToken) {
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
                return trackingToken;
            }
        };
    }

    public static void seekToCurrentPositions(Consumer<?, ?> consumer, Supplier<KafkaTrackingToken> tokenSupplier,
                                              List<String> subscriber) {
        List<TopicPartition> all = topicPartitions(consumer, subscriber);
        consumer.assign(all);
        KafkaTrackingToken currentToken = tokenSupplier.get();
        Map<TopicPartition, Long> tokenPartitionPositions = currentToken.getPositions();
        all.forEach(assignedPartition -> {
            long offset = 0L;
            if (tokenPartitionPositions.containsKey(assignedPartition)) {
                offset = tokenPartitionPositions.get(assignedPartition) + 1;
            }

            log.info("Seeking topic-partition [{}] with offset [{}]", assignedPartition, offset);
            consumer.seek(assignedPartition, offset);
        });
    }

    public static List<TopicPartition> topicPartitions(Consumer<?, ?> consumer, List<String> topics) {
        return consumer.listTopics().entrySet()
                .stream()
                .filter(e -> topics.contains(e.getKey()))
                .flatMap(e -> e.getValue().stream())
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition()))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    public static class Builder implements AssertionConcern {

        private KafkaConsumer<String, String> consumer;
        private List<String> topics;
        private String name;

        public Builder consumer(KafkaConsumer<String, String> consumer) {
            this.consumer = assertArgumentNotNull(
                    consumer,
                    "KafkaConsume",
                    "should not be null, because it is mandatory"
            );
            return this;
        }

        public Builder topics(List<String> topics) {
            assertArgumentNotNull(
                    topics,
                    "Topics",
                    "should not be null, because it is mandatory"
            );
            assertArgumentNotEmpty(
                    topics,
                    "Topics",
                    "should not be empty, because it is mandatory"
            );
            this.topics = topics;
            return this;
        }

        public Builder name(String name) {
            this.name = assertArgumentNotNull(
                    name,
                    "Name",
                    "should not be null, because it is mandatory"
            );
            return this;
        }

        public KafkaMessageSource build() {
            return new KafkaMessageSource(this);
        }
    }
}
