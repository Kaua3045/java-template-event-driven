package com.kaua.event.driven.infrastructure.es.command.callback;

import com.kaua.event.driven.infrastructure.uow.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingCallback implements CommandCallBack<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(LoggingCallback.class);

    public static final LoggingCallback INSTANCE = new LoggingCallback();

    private LoggingCallback() {}

    @Override
    public void onResult(Object command, ResultMessage<?> result) {
        if (result.isExceptional()) {
            log.warn("Command resulted in exception: {}", command, result.getExceptionResult());
        } else {
            log.info("Command executed successfully: {}", command);
        }
    }
}
