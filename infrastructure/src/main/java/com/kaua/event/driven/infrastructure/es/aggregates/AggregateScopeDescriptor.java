package com.kaua.event.driven.infrastructure.es.aggregates;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kaua.event.driven.infrastructure.es.scope.ScopeDescriptor;

import java.beans.ConstructorProperties;

public class AggregateScopeDescriptor implements ScopeDescriptor {

    private final String type;
    private Object identifier;

    @JsonCreator
    @ConstructorProperties({ "type", "identifier" })
    public AggregateScopeDescriptor(@JsonProperty("type") String type, @JsonProperty("identifier") Object identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    public String getType() {
        return type;
    }

    public Object getIdentifier() {
        return identifier;
    }

    @Override
    public String scopeDescription() {
        return String.format("AggregateScopeDescriptor for type [%s] and identifier [%s]", type, getIdentifier());
    }

    @Override
    public String toString() {
        return "AggregateScopeDescriptor{" +
                "type=" + type +
                ", identifier='" + getIdentifier() + '\'' +
                '}';
    }
}
