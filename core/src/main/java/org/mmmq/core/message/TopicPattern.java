package org.mmmq.core.message;

import org.mmmq.core.util.PatternMatcher;

/**
 * Dispatcher가 어떤 토픽을 구독할지 정하는 Ant 스타일 패턴({@code order.*}·{@code **} 등).
 *
 * <p>브로커가 메시지를 받을 때마다 각 Dispatcher의 패턴으로 대상을 고른다.
 * 빈 값을 생성 시점에 막는 이유는 그것이 아무것도 구독하지 않는 Dispatcher를 조용히 만들기 때문이다.
 */
public record TopicPattern(
        String value
) {

    private static final PatternMatcher PATH_MATCHER = new PatternMatcher();

    public TopicPattern {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("pattern must not be blank, but was: " + value);
        }
    }

    public boolean matches(Topic topic) {
        return PATH_MATCHER.match(value, topic.name());
    }
}
