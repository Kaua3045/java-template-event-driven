package com.kaua.event.driven.infrastructure.es.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "events")
public class EventEntity {

    @Id
    private String eventId;

    private String aggregateId;
    private String type;

    @Column(columnDefinition = "TEXT")
    private String data;

    private long aggregateVersion;

    private boolean published;

    public EventEntity() {
    }

    public EventEntity(String eventId, String aggregateId, String type, String data, long aggregateVersion) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.type = type;
        this.data = data;
        this.aggregateVersion = aggregateVersion;
        this.published = false;
    }

    public EventEntity(String eventId, String aggregateId, String type, String data, long aggregateVersion, boolean published) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.type = type;
        this.data = data;
        this.aggregateVersion = aggregateVersion;
        this.published = published;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public long getAggregateVersion() {
        return aggregateVersion;
    }

    public void setAggregateVersion(long aggregateVersion) {
        this.aggregateVersion = aggregateVersion;
    }

    public boolean isPublished() {
        return published;
    }

    public void setPublished(boolean published) {
        this.published = published;
    }
}
