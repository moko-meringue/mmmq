package org.mmmq.producer;

public class MessagePublishException extends RuntimeException {

    public MessagePublishException(String message, Exception e) {
        super(message, e);
    }
}
