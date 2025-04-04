package com.kaua.event.driven.infrastructure.es.eventprocessing.replay;

import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;

public interface ReplayService {

    void replay(TrackingToken trackingToken);

    boolean isReplaying(String processorName);

    <T> void processReplayRequest(T any);

    <T> void processReplayResponse(T any);
}
