package org.mmmq.publisher;

public class MessagePublishException extends RuntimeException {

    public MessagePublishException(String message, Exception e) {
        super(message, e);
    }
}
