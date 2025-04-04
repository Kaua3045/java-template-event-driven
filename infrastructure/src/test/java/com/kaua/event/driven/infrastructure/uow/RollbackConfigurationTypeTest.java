package com.kaua.event.driven.infrastructure.uow;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollbackConfigurationTypeTest extends UnitTest {

    @Test
    void testAnyExceptionsRollback() {
        RollbackConfiguration testSubject = RollbackConfigurationType.ANY_THROWABLE;
        assertTrue(testSubject.rollBackOn(new RuntimeException()));
        assertTrue(testSubject.rollBackOn(new Exception()));
        assertTrue(testSubject.rollBackOn(new AssertionError()));
    }

    @Test
    void testUncheckedExceptionsRollback() {
        RollbackConfiguration testSubject = RollbackConfigurationType.UNCHECKED_EXCEPTIONS;
        assertTrue(testSubject.rollBackOn(new RuntimeException()));
        assertFalse(testSubject.rollBackOn(new Exception()));
        assertTrue(testSubject.rollBackOn(new AssertionError()));
    }

    @Test
    void testRuntimeExceptionsRollback() {
        RollbackConfiguration testSubject = RollbackConfigurationType.RUNTIME_EXCEPTIONS;
        assertTrue(testSubject.rollBackOn(new RuntimeException()));
        assertFalse(testSubject.rollBackOn(new Exception()));
        assertFalse(testSubject.rollBackOn(new AssertionError()));
    }
}
