package com.kaua.event.driven.infrastructure.cache;

public interface Cache {

    <K, V> V get(K key);

    void put(Object key, Object value);

    void putIfAbsent(Object key, Object value);

    void remove(String key);

    boolean containsKey(String key);
}
