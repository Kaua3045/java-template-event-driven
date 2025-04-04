package com.kaua.event.driven.infrastructure.uow;

import com.kaua.event.driven.domain.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class CurrentUnitOfWorkTest extends UnitTest {

    @BeforeEach
    void setUp() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @AfterEach
    void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @Test
    void givenAnNonExistsUow_whenCallGetCurrentUow_thenThrowsException() {
        assertThrows(IllegalStateException.class, CurrentUnitOfWork::get);
    }

    @Test
    void givenAnSession_whenCallSet_thenSetUnitOfWorkCorrectly() {
        UnitOfWork<?> mockUnitOfWork = mock(UnitOfWork.class);
        CurrentUnitOfWork.set(mockUnitOfWork);
        assertSame(mockUnitOfWork, CurrentUnitOfWork.get());

        CurrentUnitOfWork.clear(mockUnitOfWork);
        assertFalse(CurrentUnitOfWork.isStarted());
    }

    @Test
    void notCurrentUnitOfWorkCommitted() {
        DefaultUnitOfWork<?> outerUoW = new DefaultUnitOfWork<>(null);
        outerUoW.start();
        new DefaultUnitOfWork<>(null).start();
        try {
            outerUoW.commit();
        } catch (IllegalStateException e) {
            return;
        }
        throw new AssertionError("The unit of work is not the current");
    }
}
