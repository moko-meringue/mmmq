package org.mmmq.broker.dispatcher;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher {

    static final int MAX_RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final Pattern pattern;
    final Set<Topic> patternCache;
    final BlockingQueue<MessageEnvelope> messageQueue;
    final ThreadPoolExecutor threadPool;
    final Worker worker;
    Sender sender;

    Dispatcher(
            String name,
            Host host,
            Pattern pattern,
            BlockingQueue<MessageEnvelope> messageQueue,
            ThreadPoolExecutor threadPool,
            Sender sender
    ) {
        this.name = name;
        this.host = host;
        this.pattern = pattern;
        this.patternCache = ConcurrentHashMap.newKeySet();
        this.messageQueue = messageQueue;
        this.worker = new Worker();
        this.threadPool = threadPool;
        this.sender = sender;
    }

    public Dispatcher(String name, Host host, Pattern pattern, ThreadPoolExecutor threadPool) {
        this(name, host, pattern, new ArrayBlockingQueue<>(1000), threadPool, Sender.from(host));
    }

    public Dispatcher(String name, Host host, Pattern pattern) {
        this(
                name,
                host,
                pattern,
                new ArrayBlockingQueue<>(1000),
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
        if (pattern.matches(topic)) {
            patternCache.add(topic);
            return true;
        }
        return false;
    }

    public void dispatch(Message message, Consumer<Throwable> onFailure) {
        Message messageWithPattern = message.withPattern(pattern);
        try {
            messageQueue.put(new MessageEnvelope(messageWithPattern, onFailure));
        } catch (InterruptedException e) {
            log.warn("Failed to dispatch message, interrupted: {}", messageWithPattern);
            onFailure.accept(e);
        }
    }

    @PostConstruct
    void start() {
        worker.start();
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
                        MessageEnvelope envelope = messageQueue.take();
                        threadPool.submit(() -> send(envelope));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.info("DispatchWorker interrupted");
                    } catch (Exception e) {
                        log.warn("Failed to dispatch message: {}", e.getMessage());
                    }
                }
            }

            void send(MessageEnvelope messageEnvelope) {
                try {
                    Message message = messageEnvelope.message();
                    sender.send(message, MAX_RETRY_COUNT);
                } catch (Exception e) {
                    messageEnvelope.handleFailure(e);
                }
            }
        }
    }
}
