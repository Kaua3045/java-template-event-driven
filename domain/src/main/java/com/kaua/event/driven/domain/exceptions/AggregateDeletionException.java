package com.kaua.event.driven.domain.exceptions;

import java.util.Collections;

public class AggregateDeletionException extends DomainException {

    public AggregateDeletionException(String aggregateIdentifier) {
        super("Aggregate with identifier %s not found. It has been deleted"
                .formatted(aggregateIdentifier), Collections.emptyList());
    }
}
