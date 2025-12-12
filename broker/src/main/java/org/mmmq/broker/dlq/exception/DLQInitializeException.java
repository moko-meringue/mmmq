package org.mmmq.broker.dlq.exception;

public class DLQInitializeException extends RuntimeException {

    public DLQInitializeException(String message, Exception exception) {
        super(message, exception);
    }
}
