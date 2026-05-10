package org.mmmq.producer;

import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.producer.exception.ProduceException;

public class Producer {

    static final int DEFAULT_MAX_RETRY_COUNT = 3;

    final Gateway gateway;
    final int maxRetryCount;

    public Producer(Host host) {
        this(host, DEFAULT_MAX_RETRY_COUNT);
    }

    public Producer(Host host, int maxRetryCount) {
        this.gateway = new Gateway(host);
        this.maxRetryCount = maxRetryCount;
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
    }
}
