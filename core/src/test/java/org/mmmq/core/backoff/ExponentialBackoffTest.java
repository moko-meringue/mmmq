package org.mmmq.core.backoff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExponentialBackoffTest {

    @Test
    @DisplayName("initialDelay가 zero이면 IllegalArgumentException을 던진다.")
    void rejectZeroInitialDelay() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ZERO, Duration.ofSeconds(1), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialDelay");
    }

    @Test
    @DisplayName("initialDelay가 음수이면 IllegalArgumentException을 던진다.")
    void rejectNegativeInitialDelay() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofMillis(-1), Duration.ofSeconds(1), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialDelay");
    }

    @Test
    @DisplayName("initialDelay가 1ms 미만이면 IllegalArgumentException을 던진다.")
    void rejectSubMillisecondInitialDelay() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofNanos(500), Duration.ofSeconds(1), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialDelay");
    }

    @Test
    @DisplayName("maxDelay가 zero이면 IllegalArgumentException을 던진다.")
    void rejectZeroMaxDelay() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofMillis(1), Duration.ZERO, 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelay");
    }

    @Test
    @DisplayName("maxDelay가 음수이면 IllegalArgumentException을 던진다.")
    void rejectNegativeMaxDelay() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(-1), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelay");
    }

    @Test
    @DisplayName("initialDelay가 maxDelay보다 크면 IllegalArgumentException을 던진다.")
    void rejectInitialGreaterThanMax() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofSeconds(2), Duration.ofSeconds(1), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialDelay must be less than or equal to maxDelay");
    }

    @Test
    @DisplayName("multiplier가 1.0 미만이면 IllegalArgumentException을 던진다.")
    void rejectMultiplierLessThanOne() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofMillis(1), Duration.ofSeconds(1), 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    @DisplayName("multiplier가 NaN이면 IllegalArgumentException을 던진다.")
    void rejectMultiplierNaN() {
        assertThatThrownBy(() -> new ExponentialBackoff(Duration.ofMillis(1), Duration.ofSeconds(1), Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    @DisplayName("multiplier가 Infinity이면 IllegalArgumentException을 던진다.")
    void rejectMultiplierInfinity() {
        assertThatThrownBy(
                () -> new ExponentialBackoff(Duration.ofMillis(1), Duration.ofSeconds(1), Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    @DisplayName("next는 현재값에 multiplier를 곱한 값을 반환한다.")
    void nextMultipliesCurrent() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                2.0
        );

        Duration result = backoff.next(Duration.ofMillis(100));

        assertThat(result).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    @DisplayName("next 결과가 maxDelay를 넘으면 maxDelay에서 캡된다.")
    void nextCapsAtMaxDelay() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                2.0
        );

        Duration result = backoff.next(Duration.ofMillis(800));

        assertThat(result).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("multiplier가 1.0이면 next는 현재값을 그대로 반환한다.")
    void nextWithMultiplierOneReturnsCurrent() {
        ExponentialBackoff backoff = new ExponentialBackoff(
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                1.0
        );

        Duration result = backoff.next(Duration.ofMillis(100));

        assertThat(result).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    @DisplayName("accessor는 생성자에 전달한 값을 그대로 반환한다.")
    void accessorReturnsConstructorArguments() {
        Duration initialDelay = Duration.ofMillis(100);
        Duration maxDelay = Duration.ofSeconds(3);
        double multiplier = 2.5;

        ExponentialBackoff backoff = new ExponentialBackoff(initialDelay, maxDelay, multiplier);

        assertThat(backoff.initialDelay()).isEqualTo(initialDelay);
        assertThat(backoff.maxDelay()).isEqualTo(maxDelay);
        assertThat(backoff.multiplier()).isEqualTo(multiplier);
    }
}
