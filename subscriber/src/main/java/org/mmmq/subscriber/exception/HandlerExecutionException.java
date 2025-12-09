package org.mmmq.subscriber.exception;

public class HandlerExecutionException extends RuntimeException {

    public HandlerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
