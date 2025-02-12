package com.kaua.event.driven.domain.exceptions;

import java.util.List;
import com.kaua.event.driven.domain.validation.Error;

public class ValidationException extends DomainException {

    private ValidationException(final List<Error> aErrors) {
        super("ValidationException", aErrors);
    }

    public static ValidationException with(final List<Error> aErrors) {
        return new ValidationException(aErrors);
    }

    public static ValidationException with(final Error aError) {
        return new ValidationException(List.of(aError));
    }
}

