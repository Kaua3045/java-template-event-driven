package com.kaua.event.driven.infrastructure.uow;

public class ResultMessage<T> {

    private final T result;
    private final Exception exception;

    private ResultMessage(T result, Exception exception) {
        this.result = result;
        this.exception = exception;
    }

    public static <T> ResultMessage<T> success(T result) {
        return new ResultMessage<>(result, null);
    }

    public static <T> ResultMessage<T> failure(Exception exception) {
        return new ResultMessage<>(null, exception);
    }

    public T getResult() {
        return result;
    }

    public boolean isExceptional() {
        return exception != null;
    }

    public Exception getExceptionResult() {
        return exception;
    }
}
