package com.kaua.event.driven.infrastructure.es.eventprocessing.token;

import com.kaua.event.driven.infrastructure.configurations.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TokenStoreImpl implements TokenStore {

    private static final Logger log = LoggerFactory.getLogger(TokenStoreImpl.class);

    private final TokenJpaRepository tokenJpaRepository;

    public TokenStoreImpl(TokenJpaRepository tokenJpaRepository) {
        this.tokenJpaRepository = tokenJpaRepository;
    }

    @Override
    public void storeToken(TrackingToken trackingToken) {
        log.debug("Storing token {}", trackingToken);

        final var aRetrievedToken = tokenJpaRepository.findByProcessorName(trackingToken.processorName());

        if (aRetrievedToken.isPresent()) {
            final var tokenEntity = aRetrievedToken.get();
            tokenEntity.setTokenClass(trackingToken.getClass().getName());
            tokenEntity.setToken(Json.writeValueAsString(trackingToken));
            tokenJpaRepository.save(tokenEntity);
            return;
        }

        TokenEntity tokenEntity = new TokenEntity();
        tokenEntity.setProcessorName(trackingToken.processorName());
        tokenEntity.setTokenClass(trackingToken.getClass().getName());
        tokenEntity.setToken(Json.writeValueAsString(trackingToken));
        tokenJpaRepository.save(tokenEntity);
    }

    @Override
    public Optional<TrackingToken> retrieveToken(String processorName) {
        log.debug("Retrieving token for processor {}", processorName);
        return tokenJpaRepository.findByProcessorName(processorName)
                .map(it -> {
                    try {
                        final var aClazz = Class.forName(it.getTokenClass());
                        return (TrackingToken) Json.readValue(it.getToken(), aClazz);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void removeToken(String processorName) {
        log.debug("Removing token for processor {}", processorName);
        tokenJpaRepository.deleteByProcessorName(processorName);
    }
}
