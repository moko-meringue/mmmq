package org.mmmq.producer.exception;

public class ProduceException extends RuntimeException {

    public ProduceException(String message) {
        super(message);
    }

    public ProduceException(String message, Exception e) {
        super(message, e);
    }
}
