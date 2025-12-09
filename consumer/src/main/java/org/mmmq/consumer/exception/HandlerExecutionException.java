package org.mmmq.consumer.exception;

public class HandlerExecutionException extends RuntimeException {

    public HandlerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
