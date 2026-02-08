package org.mmmq.broker.dlq;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimerDeadLetterQueue extends DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(TimerDeadLetterQueue.class);
    private final Worker worker;

    public TimerDeadLetterQueue(String name, DeadLetterHandler handler, int intervalMillis) {
        super(name, handler);
        this.worker = new Worker(intervalMillis);
    }

    @PostConstruct
    public void start() {
        worker.start();
    }

    @PreDestroy
    public void stop() {
        worker.stop();
    }

    private class Worker {

        private final int intervalMillis;
        private final ScheduledExecutorService scheduler;

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
                handler.handle(drainAll());
            } catch (Exception e) {
                log.error("Failed to handle dead letters in {}", name, e);
            }
        }
    }
}
