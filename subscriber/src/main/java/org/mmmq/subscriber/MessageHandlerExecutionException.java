package org.mmmq.subscriber;

public class MessageHandlerExecutionException extends RuntimeException {

    public MessageHandlerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
