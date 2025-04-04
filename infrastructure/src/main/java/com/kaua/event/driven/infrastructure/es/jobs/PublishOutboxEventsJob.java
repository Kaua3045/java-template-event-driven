package com.kaua.event.driven.infrastructure.es.jobs;

import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStatus;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.OutboxStoreEngine;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.OutboxJpaRepository;
import com.kaua.event.driven.infrastructure.uow.BatchingUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.TransactionManager;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Component
@Profile("!test-integration")
public class PublishOutboxEventsJob {

    private static final Logger log = LoggerFactory.getLogger(PublishOutboxEventsJob.class);

    private final KafkaProducer<String, String> producer;
    private final OutboxJpaRepository outboxStoreEngine;
    private final TransactionManager transactionManager;

    public PublishOutboxEventsJob(
            KafkaProducer<String, String> producer,
            OutboxJpaRepository outboxStoreEngine,
            TransactionManager transactionManager
    ) {
        this.producer = Objects.requireNonNull(producer);
        this.outboxStoreEngine = Objects.requireNonNull(outboxStoreEngine);
        this.transactionManager = Objects.requireNonNull(transactionManager);
    }

    @Scheduled(
            fixedRateString = "${es.outbox.publish-rate-minutes}",
            initialDelayString = "${es.outbox.publish-initial-delay-minutes}",
            timeUnit = TimeUnit.MINUTES
    )

    public void publish() {
        log.info("Publishing outbox events");

        transactionManager.executeInTransaction(() -> {
            final var aEvents = this.outboxStoreEngine.findTop50ByStatusOrderByOccurredOnAsc(OutboxStatus.PENDING);

            if (aEvents.isEmpty()) {
                log.info("No outbox events to publish");
                return;
            }

            aEvents.forEach(aEvent -> {
                try {
                    final var aTopic = "events"; // TODO get topic dynamically

                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            aTopic,
                            aEvent.getAggregateId(),
                            aEvent.getPayload()
                    );
                    record.headers().add("event_type", aEvent.getEventType().getBytes());
                    // TODO need add more headers, aggregateId, event_type, occurredOn, etc

                    this.producer.send(record)
                            .get(20, TimeUnit.MILLISECONDS); // TODO verify this timeout

//                    this.eventProcessor.publish(aTopic, aEvent.toDomain());
                    aEvent.setStatus(OutboxStatus.SENT);
                    this.outboxStoreEngine.save(aEvent);
                    log.info("Outbox event published: {}", aEvent);
                } catch (final Exception ex) {
                    log.error("Error publishing outbox event: {}", aEvent, ex);
                    aEvent.setStatus(OutboxStatus.FAILED);
                    this.outboxStoreEngine.save(aEvent);
                }
            });
        });
    }
}
