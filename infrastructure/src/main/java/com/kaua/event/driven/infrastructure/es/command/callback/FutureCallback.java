package com.kaua.event.driven.infrastructure.es.command.callback;

import com.kaua.event.driven.infrastructure.uow.ResultMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureCallback<C, R> extends CompletableFuture<ResultMessage<? extends R>>
        implements CommandCallBack<C, R> {

    @Override
    public void onResult(C command, ResultMessage<? extends R> result) {
        super.complete(result);
    }

    public ResultMessage<? extends R> getResult() {
        try {
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResultMessage.failure(e);
        } catch (Exception e) {
            return ResultMessage.failure(e);
        }
    }

    public ResultMessage<? extends R> getResult(long timeout, TimeUnit unit) {
        try {
            return get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResultMessage.failure(e);
        } catch (Exception e) {
            return ResultMessage.failure(e);
        }
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) {
        try {
            get(timeout, unit);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }
}
