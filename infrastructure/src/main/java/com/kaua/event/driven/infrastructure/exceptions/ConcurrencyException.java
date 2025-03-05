package com.kaua.event.driven.infrastructure.exceptions;

import com.kaua.event.driven.domain.exceptions.NoStackTraceException;

public class ConcurrencyException extends NoStackTraceException {

    public ConcurrencyException(String message) {
        super(message);
    }
}
