package com.kaua.event.driven.infrastructure.es.eventprocessing;

import com.kaua.event.driven.domain.events.DomainEvent;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;

import java.util.List;

public interface MessageSource {

    List<TrackedEventMessage<DomainEvent>> readEvents(TrackingToken trackingToken);

    void close();

    void commit();

    TrackingToken defaultTrackingToken();
}
