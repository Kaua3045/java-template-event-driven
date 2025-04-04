package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;

public interface EventProcessor {

    String getName();

    void resetTokens();

    <R> void resetToken(TrackingToken startPosition, R resetContext);

    boolean supportsReset();
}
