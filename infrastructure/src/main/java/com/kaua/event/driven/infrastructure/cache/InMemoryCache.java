package com.kaua.event.driven.infrastructure.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryCache implements Cache {

    private static final Logger log = LoggerFactory.getLogger(InMemoryCache.class);

    private final ConcurrentHashMap<Object, Object> cache = new ConcurrentHashMap<>();

    @Override
    public <K, V> V get(K key) {
        log.debug("Getting value from cache for key: {}", key);
        return (V) cache.get(key);
    }

    @Override
    public void put(Object key, Object value) {
        log.debug("Putting value in cache for key: {}, value: {}", key, value);
        cache.put(key, value);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        log.debug("Putting if absent value in cache for key: {}, value: {}", key, value);
        cache.putIfAbsent(key, value);
    }

    @Override
    public void remove(String key) {
        log.debug("Removing value from cache for key: {}", key);
        cache.remove(key);
    }

    @Override
    public boolean containsKey(String key) {
        log.debug("Checking if cache contains key: {}", key);
        return cache.containsKey(key);
    }

    public void clear() {
        log.debug("Clearing cache");
        cache.clear();
    }
}
