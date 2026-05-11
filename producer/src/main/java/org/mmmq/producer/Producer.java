package org.mmmq.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.producer.exception.ProduceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer implements AutoCloseable {

    static final int DEFAULT_MAX_RETRY_COUNT = 3;
    static final int DEFAULT_ASYNC_QUEUE_CAPACITY = 1000;
    static final int DEFAULT_BATCH_SIZE = 100;
    static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final BlockingQueue<Message> asyncQueue;
    private final Object flushMonitor = new Object();
    private final int batchSize;
    private final Duration flushInterval;
    private final Thread asyncWorker;
    final Gateway gateway;
    final int maxRetryCount;
    private volatile boolean running = true;

    public Producer(Host host) {
        this(
                host,
                DEFAULT_MAX_RETRY_COUNT,
                DEFAULT_ASYNC_QUEUE_CAPACITY,
                DEFAULT_BATCH_SIZE,
                DEFAULT_FLUSH_INTERVAL
        );
    }

    public Producer(Host host, int maxRetryCount) {
        this(
                host,
                maxRetryCount,
                DEFAULT_ASYNC_QUEUE_CAPACITY,
                DEFAULT_BATCH_SIZE,
                DEFAULT_FLUSH_INTERVAL
        );
    }

    private Producer(
            Host host,
            int maxRetryCount,
            int asyncQueueCapacity,
            int batchSize,
            Duration flushInterval
    ) {
        validatePositive(asyncQueueCapacity, "asyncQueueCapacity");
        validatePositive(batchSize, "batchSize");
        validatePositive(flushInterval, "flushInterval");

        this.gateway = new Gateway(host);
        this.maxRetryCount = maxRetryCount;
        this.asyncQueue = new ArrayBlockingQueue<>(asyncQueueCapacity);
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.asyncWorker = createAsyncWorker();
        this.asyncWorker.start();
    }

    public static Builder builder(Host host) {
        return new Builder(host);
    }

    public void produce(Message message) {
        try {
            sendWithRetry(message);
        } catch (Exception e) {
            throw new ProduceException("Failed to produce message", e);
        }
    }

    public void produceAsync(Message message) {
        if (!running) {
            throw new ProduceException("Async producer is closed");
        }
        if (!asyncQueue.offer(message)) {
            throw new ProduceException("Async producer queue is full");
        }
        if (asyncQueue.size() >= batchSize) {
            notifyWorker();
        }
    }

    private Thread createAsyncWorker() {
        Thread worker = new Thread(this::runAsyncWorker, "mmmq-producer-async-worker");
        worker.setDaemon(true);
        return worker;
    }

    private void runAsyncWorker() {
        while (running) {
            if (asyncQueue.size() < batchSize) {
                waitUntilFlush();
            }
            flushAvailableMessages();
        }
        flushAvailableMessages();
    }

    private void waitUntilFlush() {
        try {
            synchronized (flushMonitor) {
                flushMonitor.wait(flushInterval.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void flushAvailableMessages() {
        List<Message> batch = new ArrayList<>(batchSize);

        do {
            asyncQueue.drainTo(batch, batchSize);
            if (batch.isEmpty()) {
                return;
            }

            sendBatch(batch);
            batch.clear();
        } while (!asyncQueue.isEmpty());
    }

    private void sendBatch(List<Message> batch) {
        for (Message message : batch) {
            try {
                sendWithRetry(message);
            } catch (Exception e) {
                log.warn("Failed to produce message asynchronously", e);
            }
        }
    }

    private void sendWithRetry(Message message) {
        for (int retryCount = 0; retryCount <= maxRetryCount; retryCount++) {
            if (gateway.send(message).isAck()) {
                return;
            }
        }
    }

    private void notifyWorker() {
        synchronized (flushMonitor) {
            flushMonitor.notifyAll();
        }
    }

    @Override
    public void close() {
        running = false;
        notifyWorker();
        asyncWorker.interrupt();
        try {
            asyncWorker.join(flushInterval.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void validatePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void validatePositive(Duration value, String fieldName) {
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    public static class Builder {

        private final Host host;
        private int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        private int asyncQueueCapacity = DEFAULT_ASYNC_QUEUE_CAPACITY;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

        private Builder(Host host) {
            this.host = host;
        }

        public Builder maxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public Builder asyncQueueCapacity(int asyncQueueCapacity) {
            this.asyncQueueCapacity = asyncQueueCapacity;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder flushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Producer build() {
            return new Producer(host, maxRetryCount, asyncQueueCapacity, batchSize, flushInterval);
        }
    }
}
