package com.kaua.event.driven.infrastructure.configurations;

import com.kaua.event.driven.infrastructure.cache.Cache;
import com.kaua.event.driven.infrastructure.configurations.properties.eventstore.EventStoreProperties;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateCommandHandlerAnnotated;
import com.kaua.event.driven.infrastructure.es.aggregates.AggregateRepository;
import com.kaua.event.driven.infrastructure.es.aggregates.annotations.AggregateRoot;
import com.kaua.event.driven.infrastructure.es.aggregates.model.AggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.model.DefaultAggregateModel;
import com.kaua.event.driven.infrastructure.es.aggregates.repositories.CacheEventSourcingRepository;
import com.kaua.event.driven.infrastructure.es.command.CommandBus;
import com.kaua.event.driven.infrastructure.es.command.impl.SimpleCommandBus;
import com.kaua.event.driven.infrastructure.es.eventprocessing.AnnotationEventHandlerAdapter;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerMessage;
import com.kaua.event.driven.infrastructure.es.eventprocessing.invoker.SimpleEventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.messagesource.KafkaMessageSource;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.InMemoryReplayStatusStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayService;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayStatusStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.kafka.KafkaReplayService;
import com.kaua.event.driven.infrastructure.es.eventprocessing.tracking.TrackingProcessor;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStoreImpl;
import com.kaua.event.driven.infrastructure.es.eventstore.jpa.EventJpaRepository;
import com.kaua.event.driven.infrastructure.es.eventstore.jpa.JpaEventStoreEngine;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.JpaOutboxStoreEngine;
import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.OutboxJpaRepository;
import com.kaua.event.driven.infrastructure.es.interceptors.ValidationMessageInterceptor;
import com.kaua.event.driven.infrastructure.es.eventstore.EventStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TokenStore;
import com.kaua.event.driven.infrastructure.es.lock.NullLockFactory;
import com.kaua.event.driven.infrastructure.es.tests.AnnotationTest;
import com.kaua.event.driven.infrastructure.uow.RollbackConfigurationType;
import com.kaua.event.driven.infrastructure.uow.TransactionManager;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class AggregateConfig {

    @Bean
    public KafkaProducer<String, String> producer() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(configProps);
    }

    @Bean
    public KafkaConsumer<String, String> consumer() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-aggregate-processor");
//        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(configProps);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(configProps);
    }

//    @Bean
//    public ProducerFactory<String, Object> producerFactory() {
//        Map<String, Object> configProps = new HashMap<>();
//        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
//
//        return new DefaultKafkaProducerFactory<>(configProps);
//    }
//
//    @Bean
//    public KafkaTemplate<String, Object> kafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }
//
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerFactory() {
        final var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setPollTimeout(1000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    private Map<String, Object> consumerConfigs() {
        final var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @Bean
    public EventStore eventStore(
            EventStoreProperties eventStoreProperties,
            EventJpaRepository eventJpaRepository,
            OutboxJpaRepository outboxJpaRepository
    ) {
        return EventStoreImpl.builder()
                .eventStoreProperties(eventStoreProperties)
                .eventStoreEngine(new JpaEventStoreEngine(eventJpaRepository))
                .outboxStoreEngine(new JpaOutboxStoreEngine(outboxJpaRepository))
                .build();
    }

    @Bean
    public Map<Class<?>, AggregateModel<?>> aggregateModels(final ApplicationContext context) {
        return context.getBeansWithAnnotation(AggregateRoot.class)
                .values().stream()
                .collect(Collectors.toMap(
                        Object::getClass, // Use the aggregate root class as the key
                        v -> new DefaultAggregateModel<>(v.getClass()) // Create a new DefaultAggregateModel for each aggregate root
                ));
    }

    @Bean
    public Map<Class<?>, AggregateCommandHandlerAnnotated<?>> aggregateCommandHandlers(
            Map<Class<?>, AggregateModel<?>> aggregateModels,
            Map<Class<?>, CacheEventSourcingRepository<?>> eventSourcingRepositoryMap
    ) {
        return aggregateModels.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> AggregateCommandHandlerAnnotated.builder()
                                .aggregateModel(entry.getValue())
                                .repository((AggregateRepository<Object>) eventSourcingRepositoryMap.get(entry.getKey()))
                                .build()
                ));
    }

    @Bean
    public Map<Class<?>, CacheEventSourcingRepository<?>> eventSourcingRepositoryMap(
            EventStore eventStore,
            Cache cache,
            Map<Class<?>, AggregateModel<?>> aggregateModels
    ) {
        return aggregateModels.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new CacheEventSourcingRepository<>(eventStore,
                                NullLockFactory.INSTANCE, entry.getValue(), cache))
                );
    }

    @Bean
    public CommandBus commandBus(
            Map<Class<?>, AggregateCommandHandlerAnnotated<?>> aggregateCommandHandlers,
            TransactionManager transactionManager
    ) {
        final var aCommandBus = SimpleCommandBus.builder()
                .transactionManager(transactionManager)
                .commandHandlers(new ArrayList<>())
                .build();

        aCommandBus.registerDispatcherInterceptor(new ValidationMessageInterceptor<>());
        aggregateCommandHandlers.forEach((aggregateRoot, handler) -> aCommandBus.registerHandler(handler));

        return aCommandBus;
    }

    @Bean
    public SimpleEventHandlerInvoker eventHandlerInvoker(
            ApplicationContext context
    ) {
        final var aBean = context.getBeansWithAnnotation(AnnotationTest.class)
                .values()
                .stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No event handler found"));

        final var aHandlers = new ArrayList<EventHandlerMessage>();
        aHandlers.add(new AnnotationEventHandlerAdapter(aBean));

        return new SimpleEventHandlerInvoker(aHandlers);
    }

    @Bean
    public KafkaAdmin.NewTopics eventsTopic() {
        return new KafkaAdmin.NewTopics(
                new NewTopic("events", 1, (short) 1)
        );
    }

    @Bean
    public ReplayStatusStore replayStatusStore() {
        return new InMemoryReplayStatusStore();
    }

    @Bean
    public ReplayService replayService(
            KafkaProducer<String, String> producer,
            EventStore eventStore,
            TransactionManager transactionManager,
            EventHandlerInvoker eventHandlerInvoker,
            ReplayStatusStore replayStatusStore
    ) {
        return new KafkaReplayService(
                producer,
                replayStatusStore,
                eventStore,
                transactionManager,
                eventHandlerInvoker,
                "order-aggregate-processor"
        );
    }

    @Bean
    public TrackingProcessor trackingProcessor(
            SimpleEventHandlerInvoker eventHandlerInvoker,
            TokenStore tokenStore,
            TransactionManager transactionManager,
            KafkaConsumer<String, String> kafkaConsumer,
            ReplayService replayService,
            ReplayStatusStore replayStatusStore
    ) {
        return TrackingProcessor.builder()
                .name("order-aggregate-processor")
                .eventHandlerInvoker(eventHandlerInvoker)
                .tokenStore(tokenStore)
                .transactionManager(transactionManager)
                .messageSource(KafkaMessageSource.builder()
                        .consumer(kafkaConsumer)
                        .topics(Collections.singletonList("events"))
                        .name("order-aggregate-processor")
                        .build())
                .replayService(replayService)
                .replayStatusStore(replayStatusStore)
                .rollbackConfiguration(RollbackConfigurationType.ANY_THROWABLE)
                .build();
    }
}
