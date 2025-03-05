package com.kaua.event.driven.infrastructure.es.command.callback;

import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorAwareCallback<C, R> implements CommandCallBack<C, R> {

    private static final Logger log = LoggerFactory.getLogger(MonitorAwareCallback.class);

    private final MessageMonitor.MonitorCallback monitorCallBack;

    public MonitorAwareCallback(MessageMonitor.MonitorCallback monitorCallBack) {
        this.monitorCallBack = monitorCallBack;
    }

    @Override
    public void onResult(C command, ResultMessage<? extends R> result) {
        log.debug("Command {} executed with result {}", command, result);
        if (result.isExceptional()) {
            monitorCallBack.reportFailure(result.getExceptionResult());
        } else {
            monitorCallBack.reportSuccess();
        }
    }
}
