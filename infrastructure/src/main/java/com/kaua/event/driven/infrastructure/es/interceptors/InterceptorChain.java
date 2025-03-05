package com.kaua.event.driven.infrastructure.es.interceptors;

public interface InterceptorChain {

    Object process() throws Exception;
}
