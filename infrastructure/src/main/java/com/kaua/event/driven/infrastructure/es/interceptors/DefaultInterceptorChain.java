package com.kaua.event.driven.infrastructure.es.interceptors;

import com.kaua.event.driven.infrastructure.es.message.MessageHandler;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;

import java.util.Iterator;

public class DefaultInterceptorChain<T> implements InterceptorChain {

    private final MessageHandler<? super T> handler;
    private final Iterator<? extends MessageHandlerInterceptor<? super T>> chain;
    private final UnitOfWork<? extends T> unitOfWork;

    public DefaultInterceptorChain(MessageHandler<? super T> handler, Iterable<? extends MessageHandlerInterceptor<? super T>> chain, UnitOfWork<? extends T> unitOfWork) {
        this.handler = handler;
        this.chain = chain.iterator();
        this.unitOfWork = unitOfWork;
    }

    @Override
    public Object process() throws Exception {
        if (chain.hasNext()) {
            return chain.next().handle(unitOfWork, this);
        } else {
            return handler.handle(unitOfWork.getMessage());
        }
    }
}
