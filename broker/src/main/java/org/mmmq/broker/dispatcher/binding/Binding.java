package org.mmmq.broker.dispatcher.binding;

import org.mmmq.core.message.Topic;
import org.mmmq.core.util.PatternMatcher;

public record Binding(
        String pattern
) {

    private static final PatternMatcher PATH_MATCHER = new PatternMatcher();

    public boolean matches(Topic topic) {
        return PATH_MATCHER.match(pattern, topic.name());
    }
}
