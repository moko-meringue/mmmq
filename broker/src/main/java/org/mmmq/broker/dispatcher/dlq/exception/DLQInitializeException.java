package org.mmmq.broker.dispatcher.dlq.exception;

public class DLQInitializeException extends RuntimeException {

    public DLQInitializeException(String message, Exception exception) {
        super(message, exception);
    }
}
