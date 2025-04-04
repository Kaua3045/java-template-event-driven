package com.kaua.event.driven.infrastructure.es.eventprocessing.replay.kafka;

import com.kaua.event.driven.infrastructure.configurations.json.Json;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class ProcessRequestReplay {

    private static final Logger log = LoggerFactory.getLogger(ProcessRequestReplay.class);

    private final KafkaProducer<String, String> producer;
    private final EventStore eventStore;
    public String replayResponseTopic = "replay-events-response-";
    private final String processorName;

    public ProcessRequestReplay(
            KafkaProducer<String, String> producer,
            EventStore eventStore,
            String processorName
    ) {
        this.producer = producer;
        this.eventStore = eventStore;
        this.processorName = processorName;
        this.replayResponseTopic = replayResponseTopic.concat(processorName);
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
}
