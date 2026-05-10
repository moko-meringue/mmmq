package org.mmmq.producer;

import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.producer.exception.ProduceException;

public class Producer {

    static final int DEFAULT_MAX_RETRY_COUNT = 3;

    final Gateway gateway;
    final int maxRetryCount;

    public Producer(Host host) {
        this.gateway = new Gateway(host);
        this.maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
    }

    public Producer(Host host, int maxRetryCount) {
        this.gateway = new Gateway(host);
        this.maxRetryCount = maxRetryCount;
    }

    public static Builder builder(Host host) {
        return new Builder(host);
    }

    public void produce(Message message) {
        try {
            for (int retryCount = 0; retryCount <= maxRetryCount; retryCount++) {
                if (gateway.send(message).isAck()) {
                    return;
                }
            }
        } catch (Exception e) {
            throw new ProduceException("Failed to produce message", e);
        }
        throw new ProduceException("Failed to produce message after " + (maxRetryCount + 1) + " attempts");
    }

    public static class Builder {

        private final Host host;
        private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;

        private Builder(Host host) {
            this.host = host;
        }

        public Builder maxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public Producer build() {
            return new Producer(host, maxRetryCount);
        }
    }
}
