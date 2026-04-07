package org.mmmq.core.message;

public record Message(
    Topic topic,
    Object content
) {

    public Message {
        if (topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        if (content == null) {
            throw new IllegalArgumentException("content is null");
        }
    }
}
