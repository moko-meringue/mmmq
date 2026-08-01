package org.mmmq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HostTest {

    @Test
    @DisplayName("주소가 비어 있으면 IllegalArgumentException을 던진다.")
    void rejectsBlankAddress() {
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, null, 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "  ", 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("포트가 1~65535 범위를 벗어나면 IllegalArgumentException을 던진다.")
    void rejectsPortOutOfRange() {
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "localhost", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Host(WebProtocol.HTTP, "localhost", 65536))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("toUri는 이름을 IP로 바꾸지 않고 원본 주소를 그대로 쓴다.")
    void keepsOriginalAddressInUri() {
        Host host = new Host(WebProtocol.HTTPS, "consumer-host", 8443);

        assertThat(host.toUri()).isEqualTo("https://consumer-host:8443");
    }

    @Test
    @DisplayName("from은 URL 문자열을 파싱하고 스킴의 대소문자를 가리지 않는다.")
    void parsesUrl() {
        assertThat(Host.from("https://consumer-host:8443").toUri()).isEqualTo("https://consumer-host:8443");
        assertThat(Host.from("HTTP://consumer-host:8080").toUri()).isEqualTo("http://consumer-host:8080");
    }

    @Test
    @DisplayName("from에 빈 URL을 주면 IllegalArgumentException을 던진다.")
    void rejectsBlankUrl() {
        assertThatThrownBy(() -> Host.from(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Host.from("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("포트가 없는 URL은 IllegalArgumentException을 던진다.")
    void rejectsUrlWithoutPort() {
        assertThatThrownBy(() -> Host.from("http://consumer-host"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("host를 못 뽑는 URL은 IllegalArgumentException을 던진다.")
    void rejectsUrlWithoutHost() {
        assertThatThrownBy(() -> Host.from("consumer-host:8080"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("path·query·userInfo·fragment가 붙은 URL은 IllegalArgumentException을 던진다.")
    void rejectsUrlWithExtraComponents() {
        assertThatThrownBy(() -> Host.from("http://consumer-host:8080/foo"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Host.from("http://user@consumer-host:8080"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("미지원 스킴은 IllegalArgumentException을 던진다.")
    void rejectsUnsupportedScheme() {
        assertThatThrownBy(() -> Host.from("ftp://consumer-host:21"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
