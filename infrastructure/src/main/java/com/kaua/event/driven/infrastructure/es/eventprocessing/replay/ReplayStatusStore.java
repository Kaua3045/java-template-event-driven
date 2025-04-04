package com.kaua.event.driven.infrastructure.es.eventprocessing.replay;

public interface ReplayStatusStore {

    boolean isReplaying(String processorName);

    void replayStart(String processorName);

    void release(String processorName);
}
