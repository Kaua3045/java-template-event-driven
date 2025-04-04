package com.kaua.event.driven.infrastructure.es.eventprocessing.token;

import java.util.Optional;

public interface TokenStore {

    void storeToken(TrackingToken trackingToken);

    Optional<TrackingToken> retrieveToken(String processorName);

    void removeToken(String processorName);
}
