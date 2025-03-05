package com.kaua.event.driven.infrastructure.exceptions;

import com.kaua.event.driven.domain.exceptions.NoStackTraceException;

public class DeadlockException extends NoStackTraceException {

    public DeadlockException(String message) {
        super(message);
    }
}
