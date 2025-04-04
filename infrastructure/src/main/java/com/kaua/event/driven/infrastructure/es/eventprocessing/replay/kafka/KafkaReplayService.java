package com.kaua.event.driven.infrastructure.es.eventprocessing.replay.kafka;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayService;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayStatusStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;
import com.kaua.event.driven.infrastructure.es.interceptors.DefaultInterceptorChain;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.uow.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaReplayService implements ReplayService {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplayService.class);

    private final KafkaProducer<String, String> producer;
    private final ReplayStatusStore replayStatusStore;
    private final EventStore eventStore;
    private final TransactionManager transactionManager;
    private final EventHandlerInvoker eventHandlerInvoker;
    public String replayTopic = "replay-events-request";
    public String replayResponseTopic = "replay-events-response-";
    private final String processorName;

    public KafkaReplayService(
            KafkaProducer<String, String> producer,
            ReplayStatusStore replayStatusStore,
            EventStore eventStore,
            TransactionManager transactionManager,
            EventHandlerInvoker eventHandlerInvoker,
            String processorName
    ) {
        this.producer = producer;
        this.replayStatusStore = replayStatusStore;
        this.eventStore = eventStore;
        this.transactionManager = transactionManager;
        this.eventHandlerInvoker = eventHandlerInvoker;
        this.replayResponseTopic = replayResponseTopic.concat(processorName);
        this.processorName = processorName;
    }

    @Override
    public void replay(TrackingToken trackingToken) {
        log.debug("Replaying events from token {}", trackingToken);
        try {
            this.replayStatusStore.replayStart(trackingToken.processorName());

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    replayTopic,
                    Json.writeValueAsString(trackingToken)
            );

            // TODO verify this timeout
            this.producer.send(record).get(15, TimeUnit.SECONDS);

            log.info("Replay events request sent to topic [{}] with tracking token {}",
                    replayTopic, trackingToken);
        } catch (Exception ex) {
            this.replayStatusStore.release(trackingToken.processorName());
            throw new RuntimeException("Error while replaying events", ex);
        }
    }

    @Override
    public boolean isReplaying(String processorName) {
        log.debug("Checking if processor [{}] is replaying", processorName);
        return this.replayStatusStore.isReplaying(processorName);
    }

    @Override
    public <T> void processReplayRequest(T any) {
//        consumerRequest.subscribe(Collections.singletonList(replayTopic));

        try {
//            while (!Thread.currentThread().isInterrupted()) {
//                var records = consumerRequest.poll(Duration.ofMillis(15));
//
//                if (records.isEmpty()) {
//                    log.debug("No records found for replay request");
//                    continue;
//                }

            if (any instanceof ConsumerRecord<?,?> consumerRecord) {

                // TODO precisamos configurar o producer como transacional
//                producer.beginTransaction();
                ConsumerRecord<String, String> record = (ConsumerRecord<String, String>) consumerRecord;

                // TODO verificar o deserializer o tracking tokne
                final var trackingToken = Json.readValue(record.value(), KafkaTrackingToken.class);
                final var aEvents = eventStore.readFirstEvents(); // TODO o correto seria o tracking token vir com o index global (tipo, 1, 2, 3,....)

                System.out.println("events " + aEvents.size());

                // TODO aqui deveria enviar em batch, ou envia tudo de uma vez
                // ou falha fora que aqui da forma que esta, se tiver 10 reset, vai enviar 10x o mesmo evento
                aEvents.forEach(event -> {
                    ProducerRecord<String, String> aRecord = new ProducerRecord<>(
                            replayResponseTopic,
                            Json.writeValueAsString(event)
                    );

                    aRecord.headers().add("event_type", event.getClass().getName().getBytes());

                    try {
                        producer.send(aRecord).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

//                records.forEach(record -> {
//                    log.debug("Received replay request: {}", record);
//                    var trackingToken = Json.readValue(record.value(), TrackingToken.class);
//                    final var aEvents = eventStore.readFirstEvents(); // TODO o correto seria o tracking token vir com o index global (tipo, 1, 2, 3,....)
//
//                    // TODO aqui deveria enviar em batch, ou envia tudo de uma vez
//                    // ou falha
//                    aEvents.forEach(event -> {
//                        ProducerRecord<String, String> aRecord = new ProducerRecord<>(
//                                replayResponseTopic,
//                                Json.writeValueAsString(event)
//                        );
//                        producer.send(aRecord);
//                    });

                    // e ai assim passamos o tracking token para o event store
                    // ele vai pegar o index global, se for x ele busca a partir daquele, se for 0, vai do inicio
//                });
//                producer.commitTransaction();
            }
        } catch (Exception ex) {
            log.error("Error while processing replay request", ex);
//            producer.abortTransaction();
        } finally {
        }
    }

    @Override
    public <T> void processReplayResponse(T any) {
//        consumerResponse.subscribe(Collections.singletonList(replayResponseTopic));

        try {
//            while (!Thread.currentThread().isInterrupted()) {
//                var records = consumerResponse.poll(Duration.ofMillis(15));
            if (any instanceof ConsumerRecord<?,?> consumerRecord) {
                final ConsumerRecord<String, String> record = (ConsumerRecord<String, String>) consumerRecord;
                final var aDomainEvents = new ArrayList<DomainEvent>();

                final var aClazz = Class.forName(new String(record.headers().lastHeader("event_type").value()));
                final var aEvent = (DomainEvent) Json.readValue(record.value(), aClazz);

                log.debug("Replaying event: {}", aEvent);
                aDomainEvents.add(aEvent);

//                record.forEach(record -> {
//                    log.debug("Received replay response: {}", record);
//                    var aTrackedEventMessage = Json.readValue(record.value(), TrackedEventMessage.class);
//                    final var aEvent = aTrackedEventMessage.getPayload();
//
//                    log.debug("Replaying event: {}", aEvent);
//                    aDomainEvents.add(aEvent);
//                    // TODO implementar o replay do evento
//                    // pega a classe que contem os event handler
//                    // e fica chamando para cada evento, dai ela decide se processa ou não
//                    // no fim de tudo, tudo é commitado
//                    // recebemos o event invoker
//                    // criamos uma transaction global talvez (batch) e processamos
//                });

                UnitOfWork<? extends DomainEvent> unitOfWork = new BatchingUnitOfWork<>(aDomainEvents);
                unitOfWork.attachTransaction(transactionManager);
                processInUnitOfWork(aDomainEvents, unitOfWork);
                replayStatusStore.release(processorName);
            }
        } catch (Exception ex) {
            log.error("Error while processing replay response", ex);
        } finally {
//            consumerResponse.close();
        }
    }

    protected final void processInUnitOfWork(List<DomainEvent> events, UnitOfWork<? extends DomainEvent> unitOfWork) throws Exception {
        ResultMessage<?> resultMessage = unitOfWork.executeWithResult(() -> {
            DomainEvent event = unitOfWork.getMessage();
            System.out.println("Processing event: " + event);
            // callback
            return new DefaultInterceptorChain<>(
                    this::processMessage,
                    Collections.emptyList(),
                    unitOfWork
            ).process();
        }, RollbackConfigurationType.ANY_THROWABLE);

        System.out.println("Result message replay: " + resultMessage);
        System.out.println("result message result replay: " + resultMessage.getResult());

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

    protected Object processMessage(DomainEvent event) {
        try {
            log.debug("Processing event: {}", event);
            eventHandlerInvoker.handle(event);
            System.out.println("after invoke");
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(
            concurrency = "1",
            containerFactory = "kafkaListenerFactory",
            topics = "replay-events-request",
            groupId = "replay-events-request",
            id = "replay-events-request-01"
    )
    public void onMessageRequest(
            @Payload ConsumerRecord<String, String> record,
            final Acknowledgment ack
    ) {
        log.info("Received replay request: {}", record);

        try {
            if (record == null) {
                log.debug("No record found for replay request");
                return;
            }

            processReplayRequest(record);
            ack.acknowledge();
            // TODO não fez o ack, provavelmente é alguma config
            log.info("Replay request processed");
        } catch (Exception ex) {
            ack.nack(Duration.ofMillis(30));
            log.warn("Error while processing replay request", ex);
        }
    }

    @KafkaListener(
            concurrency = "1",
            containerFactory = "kafkaListenerFactory",
            topics = "replay-events-response-order-aggregate-processor",
            groupId = "replay-events-response-order-aggregate-processor",
            id = "replay-events-response-order-aggregate-processor-01"
    )
    public void onMessageResponse(
            @Payload ConsumerRecord<String, String> record,
            final Acknowledgment ack
    ) {
        log.info("Received replay response: {}", record);
        // TODO aqui teria que ser em lote

        try {
            if (record == null) {
                log.debug("No record found for replay response");
                return;
            }

            processReplayResponse(record);
            ack.acknowledge();
            log.info("Replay response processed");
        } catch (Exception ex) {
            ack.nack(Duration.ofMillis(30));
            log.warn("Error while processing replay response", ex);
        }
    }
}
