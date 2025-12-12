package org.mmmq.broker.dispatcher.exception;

public class DispatcherInitializeException extends RuntimeException {

    public DispatcherInitializeException(String message, Exception exception) {
        super(message, exception);
    }
}
