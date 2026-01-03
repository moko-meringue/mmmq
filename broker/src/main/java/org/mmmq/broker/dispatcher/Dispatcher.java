package org.mmmq.broker.dispatcher;

import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Dispatcher {

    static final int MAX_RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final List<Pattern> patterns;
    final Set<Topic> patternCache;
    final BlockingQueue<Message> messageQueue;
    final DeadLetterQueue deadLetterQueue;
    final ThreadPoolExecutor threadPool;
    final Worker worker;
    Sender sender;

    Dispatcher(
            String name,
            Host host,
            List<Pattern> patterns,
            BlockingQueue<Message> messageQueue,
            @Nullable DeadLetterQueue deadLetterQueue,
            ThreadPoolExecutor threadPool,
            Sender sender
    ) {
        this.name = name;
        this.host = host;
        this.patterns = patterns;
        this.patternCache = ConcurrentHashMap.newKeySet();
        this.messageQueue = messageQueue;
        this.deadLetterQueue = deadLetterQueue == null ? DeadLetterQueue.NO_OP : deadLetterQueue;
        this.worker = new Worker();
        this.threadPool = threadPool;
        this.sender = sender;
    }

    public Dispatcher(
            String name,
            Host host,
            List<Pattern> patterns,
            BlockingQueue<Message> messageQueue,
            ThreadPoolExecutor threadPool
    ) {
        this(name, host, patterns, new ArrayBlockingQueue<>(1000), null, threadPool, Sender.from(host));
    }

    public Dispatcher(
            String name,
            Host host,
            List<Pattern> patterns,
            @Nullable DeadLetterQueue deadLetterQueue,
            ThreadPoolExecutor threadPool
    ) {
        this(name, host, patterns, new ArrayBlockingQueue<>(1000), deadLetterQueue, threadPool, Sender.from(host));
    }

    public Dispatcher(String name, Host host, List<Pattern> patterns, @Nullable DeadLetterQueue deadLetterQueue) {
        this(
                name,
                host,
                patterns,
                new ArrayBlockingQueue<>(1000),
                deadLetterQueue,
                new ThreadPoolExecutor(
                        2,
                        5,
                        40L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(100)
                ),
                Sender.from(host)
        );
    }

    public boolean isSubscribing(Topic topic) {
        if (patternCache.contains(topic)) {
            return true;
        }
        if (patterns.stream().anyMatch(binding -> binding.matches(topic))) {
            patternCache.add(topic);
            return true;
        }
        return false;
    }

    public void dispatch(Message message) {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            log.warn("Failed to dispatch message, interrupted: {}", message);
        }
    }

    @PostConstruct
    void start() {
        worker.start();
        deadLetterQueue.start();
    }

    @PreDestroy
    void stop() {
        worker.stop();
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
        deadLetterQueue.stop();
    }

    private class Worker {

        final Thread thread;

        Worker() {
            this.thread = new Thread(new Job(), "mmmq-dispatcher" + "-" + name + "-worker");
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

        private class Job implements Runnable {

            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted() && !threadPool.isShutdown()) {
                    try {
                        Message message = messageQueue.take();
                        threadPool.submit(() -> send(message));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.info("DispatchWorker interrupted");
                    } catch (Exception e) {
                        log.warn("Failed to dispatch message: {}", e.getMessage());
                    }
                }
            }

            void send(Message message) {
                try {
                    if (!sender.send(message, MAX_RETRY_COUNT)) {
                        handleFailure(DeadLetter.maxRetriesExceeded(message));
                    }
                } catch (Exception e) {
                    handleFailure(DeadLetter.processingFailed(message, e));
                }
            }

            void handleFailure(DeadLetter deadLetter) {
                log.warn("Failed to send message: {}", deadLetter);
                deadLetterQueue.add(deadLetter);
            }
        }
    }
}
