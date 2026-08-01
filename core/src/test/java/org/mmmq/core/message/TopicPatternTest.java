package org.mmmq.core.message;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPatternTest {
    static Stream<Arguments> patternMatchesTestSource() {
        return Stream.of(
                Arguments.of(new TopicPattern("sports.*"), "sports.football", true),
                Arguments.of(new TopicPattern("sports.*"), "sports.basketball", true),
                Arguments.of(new TopicPattern("sports.*"), "news.politics", false),
                Arguments.of(new TopicPattern("news.**"), "news", true),
                Arguments.of(new TopicPattern("news.**"), "news.world.europe", true),
                Arguments.of(new TopicPattern("news.**"), "sports.football", false),
                Arguments.of(new TopicPattern("order.new"), "order.new", true)
        );
    }

    @ParameterizedTest
    @DisplayName("topic 패턴 매칭 테스트")
    @MethodSource("patternMatchesTestSource")
    void patternMatchesTest(TopicPattern pattern, String topicName, boolean expected) {
        Topic topic = new Topic(topicName);
        assertThat(pattern.matches(topic)).isEqualTo(expected);
    }

    @Test
    @DisplayName("pattern이 비어 있으면 IllegalArgumentException을 던진다")
    void rejectsBlankPattern() {
        assertThatThrownBy(() -> new TopicPattern(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopicPattern("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
