package org.mmmq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MessageTest {

    @Test
    @DisplayName("content는 null일 수 있다")
    void contentCanBeNull() {
        Message message = new Message(new Topic("topic"), null);

        assertThat(message.content()).isNull();
    }

    @Test
    @DisplayName("topic은 null일 수 없다")
    void topicCannotBeNull() {
        assertThatThrownBy(() -> new Message(null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("topic is null");
    }
}
