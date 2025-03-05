package com.kaua.event.driven.infrastructure.es.interceptors;

import com.kaua.event.driven.infrastructure.uow.UnitOfWork;

public interface MessageHandlerInterceptor<T> {

    Object handle(UnitOfWork<? extends T> unitOfWork, InterceptorChain interceptorChain) throws Exception;
}
