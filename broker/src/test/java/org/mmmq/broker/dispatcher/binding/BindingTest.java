package org.mmmq.broker.dispatcher.binding;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mmmq.core.message.Topic;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BindingTest {

    static Stream<Arguments> bindingMatchesTestSource() {
        return Stream.of(
                Arguments.of("sports.*", "sports.football", true),
                Arguments.of("sports.*", "sports.basketball", true),
                Arguments.of("sports.*", "news.politics", false),
                Arguments.of("news.**", "news", true),
                Arguments.of("news.**", "news.world.europe", true),
                Arguments.of("news.**", "sports.football", false)
        );
    }

    @ParameterizedTest
    @DisplayName("topic 패턴 매칭 테스트")
    @MethodSource("bindingMatchesTestSource")
    void bindingMatchesTest(String pattern, String topicName, boolean expected) {
        Binding binding = new Binding(pattern);
        Topic topic = new Topic(topicName);
        assertThat(binding.matches(topic)).isEqualTo(expected);
    }
}
