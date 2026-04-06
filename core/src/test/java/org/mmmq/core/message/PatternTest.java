package org.mmmq.core.message;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PatternTest {
    static Stream<Arguments> patternMatchesTestSource() {
        return Stream.of(
                Arguments.of(new Pattern("sports.*"), "sports.football", true),
                Arguments.of(new Pattern("sports.*"), "sports.basketball", true),
                Arguments.of(new Pattern("sports.*"), "news.politics", false),
                Arguments.of(new Pattern("news.**"), "news", true),
                Arguments.of(new Pattern("news.**"), "news.world.europe", true),
                Arguments.of(new Pattern("news.**"), "sports.football", false),
                Arguments.of(new Pattern("order.new"), "order.new", true)
        );
    }

    @ParameterizedTest
    @DisplayName("topic 패턴 매칭 테스트")
    @MethodSource("patternMatchesTestSource")
    void patternMatchesTest(Pattern pattern, String topicName, boolean expected) {
        Topic topic = new Topic(topicName);
        assertThat(pattern.matches(topic)).isEqualTo(expected);
    }
}
