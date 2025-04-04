package com.kaua.event.driven.infrastructure.es.eventprocessing.replay;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryReplayStatusStore implements ReplayStatusStore {

    private final ConcurrentHashMap<String, Boolean> status = new ConcurrentHashMap<>();

    @Override
    public boolean isReplaying(String processorName) {
        return this.status.getOrDefault(processorName, false);
    }

    @Override
    public void replayStart(String processorName) {
        if (this.status.containsKey(processorName)) {
            return;
        }
        this.status.put(processorName, true);
    }

    @Override
    public void release(String processorName) {
        this.status.remove(processorName);
    }
}
