package com.kaua.event.driven.infrastructure.es.eventprocessing.replay;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.kaua.event.driven.infrastructure.es.eventprocessing.TrackedEventMessage;
import com.kaua.event.driven.infrastructure.es.eventprocessing.token.TrackingToken;

import java.beans.ConstructorProperties;

public class ReplayToken implements TrackingToken {

    private final String processorName;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    private final TrackingToken tokenAtReset;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    private final TrackingToken currentToken;

    private final transient boolean lastMessageWasReplay;

    @JsonCreator
    @ConstructorProperties({"token_at_reset", "current_token", "processor_name"})
    public ReplayToken(
            @JsonProperty("token_at_reset") TrackingToken tokenAtReset,
            @JsonProperty("current_token") TrackingToken newRedeliveredToken,
            @JsonProperty("processor_name") String processorName
    ) {
        this(processorName, tokenAtReset, newRedeliveredToken, true);
    }

    private ReplayToken(
            String processorName,
            TrackingToken tokenAtReset,
            TrackingToken newRedeliveredToken,
            boolean lastMessageWasReplay
    ) {
        this.processorName = processorName;
        this.tokenAtReset = tokenAtReset;
        this.currentToken = newRedeliveredToken;
        this.lastMessageWasReplay = lastMessageWasReplay;
    }

    public static TrackingToken createReplayToken(
            TrackingToken tokenAtReset,
            TrackingToken startPosition,
            String processorName
    ) {
        if (tokenAtReset == null) {
            return startPosition;
        }

        if (tokenAtReset instanceof ReplayToken) {
            return new ReplayToken(
                    processorName,
                    ((ReplayToken) tokenAtReset).tokenAtReset,
                    startPosition,
                    false
            );
        }

        // TODO validate if startPosition is after tokenAtReset

        return new ReplayToken(tokenAtReset, startPosition, processorName);
    }

    public static boolean isReplay(TrackedEventMessage<?> event) {
        return isReplay(event.trackingToken());
    }

    public static boolean isReplay(TrackingToken trackingToken) {
        if (trackingToken instanceof ReplayToken) {
            System.out.println("Tracking token is replay " + ((ReplayToken) trackingToken).isReplay());
        }
        return trackingToken instanceof ReplayToken && ((ReplayToken) trackingToken).isReplay();
    }

    public boolean isReplay() {
        return lastMessageWasReplay;
    }

    @Override
    public String processorName() {
        return processorName;
    }

    public TrackingToken getCurrentToken() {
        return currentToken;
    }

    public TrackingToken getTokenAtReset() {
        return tokenAtReset;
    }
}
