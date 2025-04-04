package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;

public interface TrackedEventMessage<T extends DomainEvent> extends DomainEvent {

    T getPayload();

    TrackingToken trackingToken();
}
