package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.Host;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostDefinitionTest {

    @Test
    @DisplayName("정의는 Host로 변환되며 protocol은 대소문자를 가리지 않는다")
    void convertsToHostCaseInsensitively() {
        HostDefinition definition = new HostDefinition("http", "127.0.0.1", 8080);

        Host host = definition.toHost();

        assertThat(host.toUri()).isEqualTo("http://127.0.0.1:8080");
    }

    @Test
    @DisplayName("알 수 없는 protocol은 예외를 던진다")
    void rejectsUnknownProtocol() {
        HostDefinition definition = new HostDefinition("ftp", "127.0.0.1", 8080);

        assertThatThrownBy(definition::toHost)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("address가 비어 있으면 예외를 던진다")
    void rejectsBlankAddress() {
        assertThatThrownBy(() -> new HostDefinition("HTTP", " ", 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("port가 범위를 벗어나면 예외를 던진다")
    void rejectsPortOutOfRange() {
        assertThatThrownBy(() -> new HostDefinition("HTTP", "127.0.0.1", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
