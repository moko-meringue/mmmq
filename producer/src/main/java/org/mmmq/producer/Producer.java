package org.mmmq.producer;

import java.time.Duration;

import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.producer.exception.ProduceException;

public class Producer {

    static final int DEFAULT_MAX_RETRY_COUNT = 3;
    static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofMillis(100);
    static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(3);
    static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

    final Gateway gateway;
    final int maxRetryCount;
    final Duration initialBackoff;
    final Duration maxBackoff;
    final double backoffMultiplier;

    public Producer(Host host) {
        this(host, DEFAULT_MAX_RETRY_COUNT);
    }

    public Producer(Host host, int maxRetryCount) {
        this(
                host,
                maxRetryCount,
                DEFAULT_INITIAL_BACKOFF,
                DEFAULT_MAX_BACKOFF,
                DEFAULT_BACKOFF_MULTIPLIER
        );
    }

    private Producer(
            Host host,
            int maxRetryCount,
            Duration initialBackoff,
            Duration maxBackoff,
            double backoffMultiplier
    ) {
        validatePositive(initialBackoff, "initialBackoff");
        validatePositive(maxBackoff, "maxBackoff");
        validateBackoffRange(initialBackoff, maxBackoff);
        validateBackoffMultiplier(backoffMultiplier);

        this.gateway = new Gateway(host);
        this.maxRetryCount = maxRetryCount;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.backoffMultiplier = backoffMultiplier;
    }

    public static Builder builder(Host host) {
        return new Builder(host);
    }

    public void produce(Message message) {
        try {
            sendWithRetry(message);
        } catch (ProduceException e) {
            throw e;
        } catch (Exception e) {
            throw new ProduceException("Failed to produce message", e);
        }
    }

    private void sendWithRetry(Message message) throws Exception {
        Duration backoff = initialBackoff;

        for (int retryCount = 0; retryCount <= maxRetryCount; retryCount++) {
            try {
                if (gateway.send(message).isAck()) {
                    return;
                }
            } catch (Exception e) {
                if (retryCount == maxRetryCount) {
                    throw e;
                }
            }

            if (retryCount < maxRetryCount) {
                sleep(backoff);
                backoff = nextBackoff(backoff);
            }
        }

        throw new ProduceException("Failed to produce message after " + (maxRetryCount + 1) + " attempts");
    }

    private void sleep(Duration backoff) {
        try {
            Thread.sleep(backoff.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProduceException("Interrupted during producer backoff", e);
        }
    }

    private Duration nextBackoff(Duration currentBackoff) {
        long multipliedMillis = (long) (currentBackoff.toMillis() * backoffMultiplier);
        long nextMillis = Math.min(multipliedMillis, maxBackoff.toMillis());
        return Duration.ofMillis(nextMillis);
    }

    private static void validatePositive(Duration value, String fieldName) {
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void validateBackoffMultiplier(double backoffMultiplier) {
        if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be greater than or equal to 1.0");
        }
    }

    private static void validateBackoffRange(Duration initialBackoff, Duration maxBackoff) {
        if (initialBackoff.compareTo(maxBackoff) > 0) {
            throw new IllegalArgumentException("initialBackoff must be less than or equal to maxBackoff");
        }
    }

    public static class Builder {

        private final Host host;
        private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        private Duration initialBackoff = DEFAULT_INITIAL_BACKOFF;
        private Duration maxBackoff = DEFAULT_MAX_BACKOFF;
        private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

        private Builder(Host host) {
            this.host = host;
        }

        public Builder maxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public Builder initialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
            return this;
        }

        public Builder maxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        public Builder backoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }

        public Producer build() {
            return new Producer(host, maxRetryCount, initialBackoff, maxBackoff, backoffMultiplier);
        }
    }
}
