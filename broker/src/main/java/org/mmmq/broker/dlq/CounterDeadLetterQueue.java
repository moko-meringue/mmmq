package org.mmmq.broker.dlq;

import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CounterDeadLetterQueue extends DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(CounterDeadLetterQueue.class);

    private final int capacity;
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1, 100L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    public CounterDeadLetterQueue(String name, DeadLetterHandler handler, int capacity) {
        super(name, handler);
        this.capacity = capacity;
    }

    @PreDestroy
    public void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    @Override
    public void add(DeadLetter deadLetter) {
        super.add(deadLetter);
        if (queue.size() >= capacity) {
            handleDeadLetters();
        }
    }

    void handleDeadLetters() {
        try {
            executor.submit(() -> handler.handle(drainAll()));
        } catch (Exception e) {
            log.error("Failed to handle dead letters in {}", name, e);
        }
    }
}
