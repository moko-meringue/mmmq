package org.mmmq.producer;

import java.time.Duration;

import org.mmmq.core.Host;
import org.mmmq.core.backoff.ExponentialBackoff;
import org.mmmq.core.message.Message;
import org.mmmq.producer.exception.ProduceException;

public class Producer {

    static final int DEFAULT_MAX_RETRY_COUNT = 3;
    static final ExponentialBackoff DEFAULT_BACKOFF = new ExponentialBackoff(
            Duration.ofMillis(100),
            Duration.ofSeconds(3),
            2.0
    );

    final Gateway gateway;
    final int maxRetryCount;
    final ExponentialBackoff backoff;

    public Producer(Host host) {
        this(host, DEFAULT_MAX_RETRY_COUNT, DEFAULT_BACKOFF);
    }

    public Producer(Host host, int maxRetryCount) {
        this(host, maxRetryCount, DEFAULT_BACKOFF);
    }

    public Producer(Host host, ExponentialBackoff backoff) {
        this(host, DEFAULT_MAX_RETRY_COUNT, backoff);
    }

    public Producer(Host host, int maxRetryCount, ExponentialBackoff backoff) {
        this.gateway = new Gateway(host);
        this.maxRetryCount = maxRetryCount;
        this.backoff = backoff;
    }

    public void produce(Message message) {
        try {
            sendWithRetry(message);
        } catch (ProduceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ProduceException("Failed to produce message", e);
        }
    }

    private void sendWithRetry(Message message) {
        Duration currentDelay = backoff.initialDelay();

        for (int retryCount = 0; retryCount <= maxRetryCount; retryCount++) {
            try {
                if (gateway.send(message).isAck()) {
                    return;
                }
            } catch (RuntimeException e) {
                if (retryCount == maxRetryCount) {
                    throw e;
                }
                sleep(currentDelay);
                currentDelay = backoff.next(currentDelay);
            }
        }

        throw new ProduceException("Failed to produce message after " + (maxRetryCount + 1) + " attempts");
    }

    private void sleep(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProduceException("Interrupted during producer backoff", e);
        }
    }
}
