package com.kaua.event.driven.infrastructure.configurations.properties.eventstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "es.eventstore")
public class EventStoreProperties implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(EventStoreProperties.class);

    private boolean outboxEnabled;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("EventStoreProperties initialized: {}", this);
    }

    public boolean isOutboxEnabled() {
        return outboxEnabled;
    }

    public void setOutboxEnabled(boolean outboxEnabled) {
        this.outboxEnabled = outboxEnabled;
    }

    @Override
    public String toString() {
        return "EventStoreProperties)" +
                "outboxEnabled=" + outboxEnabled +
                ")";
    }
}
