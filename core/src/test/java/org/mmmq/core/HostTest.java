package org.mmmq.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostTest {

    @Test
    @DisplayName("Host 생성 시 호스트 연결 검증을 진행한다.")
    void convertAddressWhenCreateTest() {
        assertThatCode(() -> new Host(WebProtocol.HTTP, "localhost", 8080) {
            @Override
            public boolean healthCheck(InetAddress host) {
                return true;
            }
        }).doesNotThrowAnyException();

        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "localhost", 8080) {
            @Override
            public boolean healthCheck(InetAddress host) {
                return false;
            }
        }).isInstanceOf(IllegalArgumentException.class);
    }
}
