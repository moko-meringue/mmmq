package org.mmmq.core.message;

import org.mmmq.core.util.PatternMatcher;

public record Pattern(
        String value
) {
    private static final PatternMatcher PATH_MATCHER = new PatternMatcher();

    public boolean matches(Topic topic) {
        return PATH_MATCHER.match(value, topic.name());
    }
}
