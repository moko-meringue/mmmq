package org.mmmq.broker.dispatcher.dlq.handler.exception;

public class DeadLetterHandlingException extends RuntimeException {
    
    public DeadLetterHandlingException(String message, Exception exception) {
        super(message, exception);
    }
}
