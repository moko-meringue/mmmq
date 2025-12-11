package org.mmmq.broker.dispatcher;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mmmq.broker.dispatcher.dlq.DeadLetter;
import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dispatcher.sender.SenderFactory;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher {

    static final int MAX_RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final Set<Topic> topics;
    final LinkedBlockingQueue<Map.Entry<Message, Integer>> messageQueue;
    Sender sender;
    final ThreadPoolExecutor threadPool;
    final Thread worker;
    final DeadLetterQueue deadLetterQueue;

    public Dispatcher(
        String name,
        Host host,
        Set<Topic> topics,
        ThreadPoolExecutor threadPool,
        DeadLetterQueue deadLetterQueue
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.threadPool = threadPool;
        this.deadLetterQueue = deadLetterQueue;
        this.sender = SenderFactory.create(host);
        this.worker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !threadPool.isShutdown()) {
                try {
                    Map.Entry<Message, Integer> messageEntry = messageQueue.take();
                    if (messageEntry.getValue() > MAX_RETRY_COUNT) {
                        continue;
                    }
                    threadPool.submit(() -> {
                        Message message = messageEntry.getKey();
                        try {
                            if (!sender.send(message, MAX_RETRY_COUNT)) {
                                DeadLetter deadLetter = DeadLetter.maxRetriesExceeded(message);
                                log.warn("Failed to send message: {}", deadLetter);
                                deadLetterQueue.add(deadLetter);
                            }
                        } catch (Exception e) {
                            DeadLetter deadLetter = DeadLetter.processingFailed(message, e);
                            log.warn("Failed to send message: {}", deadLetter);
                            deadLetterQueue.add(deadLetter);
                        }
                    });
                } catch (Exception e) {
                    log.warn("Failed to dispatch message: {}", e.getMessage());
                }
            }
        });
    }

    void start() {
        worker.start();
    }

    void stop() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
    }

    public void push(Message message) {
        messageQueue.add(Map.entry(message, 0));
    }

    public boolean isSubscribing(Topic topic) {
        return topics.contains(topic);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Dispatcher that)) {
            return false;
        }
        return Objects.equals(name, that.name) && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host);
    }
}
