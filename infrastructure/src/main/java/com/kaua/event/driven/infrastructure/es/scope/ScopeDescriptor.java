package com.kaua.event.driven.infrastructure.es.scope;

import java.io.Serializable;

public interface ScopeDescriptor extends Serializable {

    String scopeDescription();
}
