package com.kaua.event.driven.infrastructure.utils;

import com.kaua.event.driven.domain.exceptions.DomainException;
import com.kaua.event.driven.domain.validation.Error;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public record ApiError(String message, List<Error> errors, Instant timestamp) {

    public static ApiError from(final DomainException ex, final Instant timestamp) {
        return new ApiError(ex.getMessage(), ex.getErrors(), timestamp);
    }

    public static ApiError from(final String message, final Instant timestamp) {
        return new ApiError(message, Collections.emptyList(), timestamp);
    }
}
