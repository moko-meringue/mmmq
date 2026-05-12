package org.mmmq.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HostTest {

    @Test
    @DisplayName("유효한 호스트 이름으로 Host를 생성할 수 있다.")
    void createWithValidHost() {
        assertThatCode(() -> new Host(WebProtocol.HTTP, "localhost", 8080))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("알 수 없는 호스트 이름이면 IllegalArgumentException을 던진다.")
    void createWithUnknownHost() {
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "invalid..host..name", 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
