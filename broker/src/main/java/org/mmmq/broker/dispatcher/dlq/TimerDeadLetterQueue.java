package org.mmmq.broker.dispatcher.dlq;

import org.mmmq.broker.dispatcher.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TimerDeadLetterQueue extends DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(TimerDeadLetterQueue.class);
    protected final BlockingQueue<DeadLetter> deadLetterQueue = new LinkedBlockingQueue<>();
    private final Worker worker;

    public TimerDeadLetterQueue(String name, DeadLetterHandler handler, int intervalMillis) {
        super(name, handler);
        this.worker = new Worker(intervalMillis);
    }

    @Override
    public void add(DeadLetter deadLetter) {
        deadLetterQueue.add(deadLetter);
    }

    @Override
    public void start() {
        worker.start();
    }

    @Override
    public void stop() {
        worker.stop();
    }

    private class Worker {

        private final ScheduledExecutorService scheduler;
        private final int intervalMillis;

        Worker(int intervalMillis) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                    job -> new Thread(job, "mmmq-dlq-" + name + "-worker")
            );
            this.intervalMillis = intervalMillis;
        }

        void start() {
            scheduler.scheduleWithFixedDelay(
                    this::handleDeadLetters,
                    intervalMillis, intervalMillis, TimeUnit.MILLISECONDS
            );
        }

        void stop() {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
            log.info("Flushing remaining dead letters...");
            handleDeadLetters();
        }

        void handleDeadLetters() {
            try {
                List<DeadLetter> deadLetters = new ArrayList<>();
                deadLetterQueue.drainTo(deadLetters);
                if (!deadLetters.isEmpty()) {
                    handler.handle(deadLetters);
                }
            } catch (Exception e) {
                log.error("Failed to handle dead letters", e);
            }
        }
    }
}
