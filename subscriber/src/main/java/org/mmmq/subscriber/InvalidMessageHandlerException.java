package org.mmmq.subscriber;

public class InvalidMessageHandlerException extends RuntimeException {

    public InvalidMessageHandlerException(String message) {
        super(message);
    }
}
