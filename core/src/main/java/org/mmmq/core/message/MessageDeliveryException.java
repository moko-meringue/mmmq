package org.mmmq.core.message;

public class MessageDeliveryException extends RuntimeException {

    public MessageDeliveryException(String message) {
        super(message);
    }

    public MessageDeliveryException(Throwable cause) {
        super(cause);
    }

    public MessageDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
