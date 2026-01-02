package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Topic;
import org.springframework.util.AntPathMatcher;

public record Binding(
    String pattern
) {

    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher(".");

    public boolean matches(Topic topic) {
        return PATH_MATCHER.match(pattern, topic.name());
    }
}
