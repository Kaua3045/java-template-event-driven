package com.kaua.event.driven.infrastructure.es.interceptors;

import com.kaua.event.driven.domain.commands.Command;
import com.kaua.event.driven.infrastructure.es.message.MessageHandlingMember;
import com.kaua.event.driven.infrastructure.uow.UnitOfWork;

public class AnnotatedCommandHandlerInterceptor<T> implements MessageHandlerInterceptor<Command> {

    private final MessageHandlingMember<T> delegate;
    private final T target;

    public AnnotatedCommandHandlerInterceptor(MessageHandlingMember<T> delegate, T target) {
        this.delegate = delegate;
        this.target = target;
    }

    @Override
    public Object handle(UnitOfWork<? extends Command> unitOfWork, InterceptorChain interceptorChain) throws Exception {
        return delegate.canHandle(unitOfWork.getMessage())
                ? delegate.handle(unitOfWork.getMessage(), target)
                : interceptorChain.process();
    }
}
