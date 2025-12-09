package org.mmmq.core.message;

import java.util.Map;

public record Message(
    Topic topic,
    Map<String, Object> content
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
