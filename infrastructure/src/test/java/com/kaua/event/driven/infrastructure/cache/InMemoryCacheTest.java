package com.kaua.event.driven.infrastructure.cache;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InMemoryCacheTest extends UnitTest {

    private static InMemoryCache inMemoryCache = new InMemoryCache();

    @AfterEach
    void tearDown() {
        inMemoryCache.clear();
    }

    @Test
    void givenAValidKeyAndValue_whenPut_thenShouldStoreTheValue() {
        final var aKey = "key";
        final var aValue = "value";

        Assertions.assertDoesNotThrow(() -> inMemoryCache.put(aKey, aValue));

        Assertions.assertEquals(aValue, inMemoryCache.get(aKey));
    }

    @Test
    void givenAValidKey_whenRemove_thenShouldRemoveTheValue() {
        final var aKey = "key";
        final var aValue = "value";

        inMemoryCache.put(aKey, aValue);

        Assertions.assertDoesNotThrow(() -> inMemoryCache.remove(aKey));

        Assertions.assertNull(inMemoryCache.get(aKey));
    }

    @Test
    void givenAValidKey_whenGet_thenShouldReturnTheValue() {
        final var aKey = "key";
        final var aValue = "value";

        inMemoryCache.put(aKey, aValue);

        Assertions.assertEquals(aValue, inMemoryCache.get(aKey));
    }

    @Test
    void givenAValidKey_whenGet_thenShouldReturnNull() {
        final var aKey = "key";

        Assertions.assertNull(inMemoryCache.get(aKey));
    }

    @Test
    void givenAValidKeyAndValue_whenPutIfAbsent_thenShouldStoreTheValue() {
        final var aKey = "key";
        final var aValue = "value";

        Assertions.assertDoesNotThrow(() -> inMemoryCache.putIfAbsent(aKey, aValue));

        Assertions.assertEquals(aValue, inMemoryCache.get(aKey));
    }

    @Test
    void givenAValidKey_whenContainsKey_thenShouldReturnTrue() {
        final var aKey = "key";
        final var aValue = "value";

        inMemoryCache.put(aKey, aValue);

        Assertions.assertTrue(inMemoryCache.containsKey(aKey));
    }

    @Test
    void givenAValidKey_whenContainsKey_thenShouldReturnFalse() {
        final var aKey = "key";

        Assertions.assertFalse(inMemoryCache.containsKey(aKey));
    }
}
