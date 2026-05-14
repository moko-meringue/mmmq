package org.mmmq.core.backoff;

import java.time.Duration;

public record ExponentialBackoff(
        Duration initialDelay,
        Duration maxDelay,
        double multiplier
) {

    public ExponentialBackoff {
        if (initialDelay.isZero() || initialDelay.isNegative() || initialDelay.toMillis() <= 0) {
            throw new IllegalArgumentException("initialDelay must be positive");
        }
        if (maxDelay.isZero() || maxDelay.isNegative() || maxDelay.toMillis() <= 0) {
            throw new IllegalArgumentException("maxDelay must be positive");
        }
        if (initialDelay.compareTo(maxDelay) > 0) {
            throw new IllegalArgumentException("initialDelay must be less than or equal to maxDelay");
        }
        if (!Double.isFinite(multiplier) || multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be greater than or equal to 1.0");
        }
    }

    public Duration next(Duration current) {
        long nextMillis = Math.min((long) (current.toMillis() * multiplier), maxDelay.toMillis());
        return Duration.ofMillis(nextMillis);
    }
}
