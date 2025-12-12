package org.mmmq.broker.dispatcher.dlq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CounterDeadLetterQueue extends DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(CounterDeadLetterQueue.class);

    private final int capacity;
    private final Worker worker = new Worker(super.name);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final Path storagePath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CounterDeadLetterQueue(String name, int capacity, Path storagePath) {
        super(name);
        this.capacity = capacity;
        this.storagePath = storagePath;
    }

    @Override
    public void add(DeadLetter deadLetter) {
        super.add(deadLetter);
        counter.incrementAndGet();
        if (canWrite()) {
            worker.write();
        }
    }

    private boolean canWrite() {
        return counter.get() >= capacity;
    }

    @Override
    void start() {
        worker.start();
    }

    @Override
    void stop() {
        worker.stop();
    }

    private class Worker {

        final Thread thread;
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();

        Worker(String name) {
            this.thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    await();
                    writeDeadLettersToFile();
                }
            }, "mmmq-dlq" + "-" + name + "-worker");
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

        void write() {
            signal();
        }

        void writeDeadLettersToFile() {
            try {
                List<DeadLetter> deadLetters = new ArrayList<>();
                deadLetterQueue.drainTo(deadLetters);
                
                if (!deadLetters.isEmpty()) {
                    String fileName = generateFileName();
                    Path filePath = storagePath.resolve(fileName);
                    
                    Files.createDirectories(storagePath);
                    String jsonContent = objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(deadLetters);
                    
                    Files.writeString(filePath, jsonContent, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    
                    counter.addAndGet(-deadLetters.size());
                }
            } catch (IOException e) {
                log.error("Failed to write dead letters to file", e);
            }
        }

        private String generateFileName() {
            return String.format("dead-letters-%s.json", name);
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
        }
    }
}
