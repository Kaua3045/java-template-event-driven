package com.kaua.event.driven.infrastructure.exceptions;

import com.kaua.event.driven.domain.exceptions.NoStackTraceException;

public class LockAcquisitionFailedException extends NoStackTraceException {

    public LockAcquisitionFailedException(String message) {
        super(message);
    }

    public LockAcquisitionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
