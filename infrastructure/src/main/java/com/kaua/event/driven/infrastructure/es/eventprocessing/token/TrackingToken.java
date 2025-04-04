package com.kaua.event.driven.infrastructure.es.eventprocessing.token;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = KafkaTrackingToken.class, name = "com.kaua.event.driven.infrastructure.es.eventprocessing.token.kafka.KafkaTrackingToken"),
        @JsonSubTypes.Type(value = KafkaTrackingToken.class, name = "com.kaua.event.driven.infrastructure.es.eventprocessing.replay.ReplayToken")
})
public interface TrackingToken {

    // TODO in future add firstToken, lastToken (representing, for example, the first and last event in a stream) on call this method create or return a correct tracking token

    String processorName();
}
