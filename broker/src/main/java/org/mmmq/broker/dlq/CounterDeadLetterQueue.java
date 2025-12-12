package org.mmmq.broker.dlq;

import org.mmmq.broker.dlq.handler.DeadLetterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CounterDeadLetterQueue extends DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(CounterDeadLetterQueue.class);
    protected final BlockingQueue<DeadLetter> deadLetterQueue = new LinkedBlockingQueue<>();
    private final int capacity;
    private final Worker worker;
    private final AtomicInteger counter = new AtomicInteger(0); // MOKO: 이거 필요없는거같은데..? size() 쓰면 안됨?

    public CounterDeadLetterQueue(String name, DeadLetterHandler handler, int capacity) {
        super(name, handler);
        this.capacity = capacity;
        this.worker = new Worker();
    }

    @Override
    public void add(DeadLetter deadLetter) {
        deadLetterQueue.add(deadLetter);
        counter.incrementAndGet();
        if (canWrite()) {
            worker.work();
        }
    }

    private boolean canWrite() {
        return counter.get() >= capacity;
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

        final Thread thread;
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();

        Worker() {
            this.thread = new Thread(
                    () -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            await();
                            handleDeadLetters();
                        }
                    }, "mmmq-dlq" + "-" + name + "-worker"
            );
        }

        void await() {
            lock.lock();
            try {
                while (!canWrite()) {
                    condition.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("CounterDeadLetterQueueWorker interrupted");
            } finally {
                lock.unlock();
            }
        }

        void signal() {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void work() {
            signal();
        }

        void handleDeadLetters() {
            try {
                List<DeadLetter> deadLetters = new ArrayList<>();
                deadLetterQueue.drainTo(deadLetters);
                handler.handle(deadLetters);
                counter.addAndGet(-deadLetters.size());
            } catch (Exception e) {
                log.error("Failed to handle dead letters", e);
            }
        }

        void start() {
            thread.start();
        }

        void stop() {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("Flushing remaining dead letters...");
            handleDeadLetters();
        }
    }
}
