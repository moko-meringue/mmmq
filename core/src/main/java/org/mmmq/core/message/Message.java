package org.mmmq.core.message;

import java.util.Map;

public record Message(
    Topic topic,
    Map<String, Object> content,
    Pattern pattern
) {

    public Message(Topic topic, Map<String, Object> content) {
        this(topic, content, null);
    }

    public Message {
        if (topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        if (content == null) {
            throw new IllegalArgumentException("content is null");
        }
    }

    public Message withPattern(Pattern pattern) {
        return new Message(topic, content, pattern);
    }
}
