package com.kaua.event.driven.infrastructure.es.eventprocessing.tracking;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.AbstractEventProcessor;
import com.kaua.event.driven.infrastructure.es.eventprocessing.EventHandlerInvoker;
import com.kaua.event.driven.infrastructure.es.eventprocessing.MessageSource;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayService;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayStatusStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TokenStore;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;
import com.kaua.event.driven.infrastructure.uow.BatchingUnitOfWork;
import com.kaua.event.driven.infrastructure.uow.RollbackConfiguration;
import com.kaua.event.driven.infrastructure.uow.TransactionManager;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TrackingProcessor extends AbstractEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TrackingProcessor.class);

    private final MessageSource messageSource;
    private final TokenStore tokenStore;
    private final TransactionManager transactionManager;
    private final ReplayService replayService;
    private final ReplayStatusStore replayStatusStore;
    private final ExecutorService executorService;
    private final String lastTokenResourceKey;

    public static Builder builder() {
        return new Builder();
    }

    public TrackingProcessor(Builder builder) {
        super(builder);
        this.messageSource = builder.messageSource;
        this.tokenStore = builder.tokenStore;
        this.transactionManager = builder.transactionManager;
        this.replayService = builder.replayService;
        this.replayStatusStore = builder.replayStatusStore;
        this.executorService = builder.executorService;
        this.lastTokenResourceKey = "LAST_TOKEN_" + getName();

        registerHandlerInterceptor(((unitOfWork, interceptorChain) -> {
            if (!(unitOfWork instanceof BatchingUnitOfWork) || ((BatchingUnitOfWork<?>) unitOfWork).isFirstMessage()) {
                // TODO verify this
                TrackingToken lastToken = (TrackingToken) unitOfWork.resources().get(lastTokenResourceKey);
                // check if store before or after
                unitOfWork.onPrepareCommit(uow -> {
                    tokenStore.storeToken(lastToken);
                });
            }
            return interceptorChain.process();
        }));
    }

    public void start() {
        executorService.submit(() -> {
            List<TrackedEventMessage<?>> batch = new ArrayList<>();
            try {
                // TODO e aqui alem de verifcar se nao esta em replaying, poderiamos verificar qual a instancia que deu start
                while (!this.replayStatusStore.isReplaying(getName())) {
                    // TODO check if need call DB every time, check if need call DB every time
                    TrackingToken trackingToken = tokenStore.retrieveToken(getName())
                            .orElse(messageSource.defaultTrackingToken());

                    System.out.println("loaded " + trackingToken);

                    List<TrackedEventMessage<DomainEvent>> events = messageSource.readEvents(trackingToken);
                    if (events.isEmpty()) {
                        log.debug("No records found for tracking token {}", trackingToken);
                        continue;
                    }

                    for (TrackedEventMessage<?> event : events) {
                        batch.add(event);
                        trackingToken = event.trackingToken();
                    }

                    System.out.println("last token: " + trackingToken);

                    processEventWithUnitOfWork(batch, trackingToken);
                    log.debug("Processed {} events", batch.size());
                    batch.clear();
                }

                Thread.sleep(Duration.ofMillis(10));
            } catch (WakeupException e) {
                log.warn("Tracking processor [{}] was interrupted. Shutting down.", getName(), e);
            } catch (Exception e) {
                log.error("Tracking processor [{}] unexpected failed to process events", getName(), e);
            }
        });
    }

    private void processEventWithUnitOfWork(List<TrackedEventMessage<?>> events, TrackingToken finalLastToken) throws Exception {
        if (events.isEmpty()) {
            return;
        }

        UnitOfWork<? extends DomainEvent> unitOfWork = new BatchingUnitOfWork<>(events);
        unitOfWork.attachTransaction(transactionManager);
        unitOfWork.resources().put(lastTokenResourceKey, finalLastToken);
        processInUnitOfWork(events, unitOfWork);
        // on error need retry
    }

    @Override
    public void resetTokens() {
        resetToken(messageSource.defaultTrackingToken(), null);
    }

    @Override
    public <R> void resetToken(TrackingToken startPosition, R resetContext) {
        // TODO preciso garantir que o job seja parado
        log.debug("Starting reset of the tracking processor [{}]", getName());
        stop();

        transactionManager.executeInTransaction(() -> {
            final var aTokenRetrieved = tokenStore.retrieveToken(getName());
            eventHandlerInvoker().performReset();
            this.tokenStore.removeToken(getName());

            tokenStore.storeToken(ReplayToken.createReplayToken(
                    aTokenRetrieved.orElse(null), // TODO verify this
                    startPosition,
                    getName()
            ));
        });

        replayStatusStore.release(getName());
    }

    public void stop() {
        try {
            log.debug("Stopping TrackingProcessor [{}]", getName());
            this.replayStatusStore.replayStart(getName());
            // TODO need actions?
        } catch (Exception ex) {
            log.error("Error on stopping TrackingProcessor [{}]", getName(), ex);
        }
    }

    @Override
    public boolean supportsReset() {
        return eventHandlerInvoker().supportsReset();
    }

    public static class Builder extends AbstractEventProcessor.Builder {

        private MessageSource messageSource;
        private TokenStore tokenStore;
        private TransactionManager transactionManager;
        private ReplayService replayService;
        private ReplayStatusStore replayStatusStore;
        private ExecutorService executorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("event-processor-" + Thread.currentThread().threadId()).factory());

        @Override
        public Builder name(String name) {
            super.name(name);
            return this;
        }

        @Override
        public Builder eventHandlerInvoker(EventHandlerInvoker eventHandlerInvoker) {
            super.eventHandlerInvoker(eventHandlerInvoker);
            return this;
        }

        @Override
        public Builder rollbackConfiguration(RollbackConfiguration rollbackConfiguration) {
            super.rollbackConfiguration(rollbackConfiguration);
            return this;
        }

        // TODO faltou os interceptors

        public Builder messageSource(MessageSource messageSource) {
            assertArgumentNotNull(messageSource, "messageSource", "should not be null because is hard dependency");
            this.messageSource = messageSource;
            return this;
        }

        public Builder tokenStore(TokenStore tokenStore) {
            assertArgumentNotNull(tokenStore, "tokenStore", "should not be null because is hard dependency");
            this.tokenStore = tokenStore;
            return this;
        }

        public Builder transactionManager(TransactionManager transactionManager) {
            assertArgumentNotNull(transactionManager, "transactionManager", "should not be null because is hard dependency");
            this.transactionManager = transactionManager;
            return this;
        }

        public Builder replayService(ReplayService replayService) {
            assertArgumentNotNull(replayService, "replayService", "should not be null because is hard dependency");
            this.replayService = replayService;
            return this;
        }

        public Builder replayStatusStore(ReplayStatusStore replayStatusStore) {
            assertArgumentNotNull(replayStatusStore, "replayStatusStore", "should not be null because is hard dependency");
            this.replayStatusStore = replayStatusStore;
            return this;
        }

        public Builder executorService(ExecutorService executorService) {
            assertArgumentNotNull("executorService", "executorService", "should not be null because is hard dependency");
            this.executorService = executorService;
            return this;
        }

        public TrackingProcessor build() {
            return new TrackingProcessor(this);
        }
    }
}
