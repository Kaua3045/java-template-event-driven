package com.kaua.event.driven.infrastructure.es.parameters;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FixedParameterResolverTest extends UnitTest {

    @Test
    void givenAValidObject_whenCallResolve_thenShouldReturnObject() {
        final var aObject = new Object();

        final var aFixedResolver = new FixedParameterResolver<>(aObject);

        final var aResolved = aFixedResolver.resolve(aObject);

        Assertions.assertEquals(aObject, aResolved);
        Assertions.assertTrue(aFixedResolver.matches(aObject));
        Assertions.assertTrue(aFixedResolver.supportedPayloadType().equals(Object.class));
    }
}
