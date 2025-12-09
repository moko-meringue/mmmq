package org.mmmq.broker.dispatcher;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dispatcher.sender.SenderFactory;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;

public class Dispatcher {

    static final int MAX_RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    final String name;
    final Host host;
    final Set<Topic> topics;
    final LinkedBlockingQueue<Map.Entry<Message, Integer>> messageQueue;
    final Sender sender;
    final ThreadPoolExecutor threadPoolExecutor;
    final Thread worker;

    public Dispatcher(
            String name,
            Host host,
            Set<Topic> topics,
            ThreadPoolExecutor threadPool
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.threadPoolExecutor = threadPool;
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
                        if (!sender.send(message).isAck()) {
                            messageQueue.add(Map.entry(message, messageEntry.getValue() + 1));
                        }
                    });
                } catch (Exception e) {
                    log.warn("Failed to send message: {}", e.getMessage());
                }
            }
        });
    }

    @PostConstruct
    public void startWorker() {
        this.worker.start();
    }

    public void push(Message message) {
        messageQueue.add(Map.entry(message, 0));
    }

    public boolean isSubscribing(String topic) {
        return topics.contains(new Topic(topic));
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

    public static class Builder {

        private static final ThreadPoolExecutor DEFAULT_THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(
                2,
                5,
                40L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );

        static {
            DEFAULT_THREAD_POOL_EXECUTOR.allowCoreThreadTimeOut(true);
        }

        private final String name;
        private final Host host;
        private final Set<Topic> subscribed = new HashSet<>();
        private ThreadPoolExecutor threadPool = DEFAULT_THREAD_POOL_EXECUTOR;

        public Builder(String name, String webProtocol, String hostName, int port) {
            this.name = name;
            this.host = new Host(webProtocol, hostName, port);
        }

        public Builder withTopics(String... topics) {
            for (String topic : topics) {
                this.subscribed.add(new Topic(topic));
            }
            return this;
        }

        public Builder threadPool(ThreadPoolExecutor executor) {
            this.threadPool = executor;
            return this;
        }

        public Dispatcher build() {
            return new Dispatcher(
                    name,
                    host,
                    subscribed,
                    threadPool
            );
        }
    }
}
