package org.mmmq.broker.dispatcher;

import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Dispatcher {

    static final int MAX_NACK_RETRY_COUNT = 3;
    static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    static final long MAX_BACKOFF_DELAY_MS = 60000;
    static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final ConcurrentHashMap<Topic, AtomicLong> offsets = new ConcurrentHashMap<>();
    final Worker worker;
    Sender sender;
    TopicQueueRegistry registry;
    ObjectProvider<DeadLetterQueue> dlqProvider;

    public Dispatcher(String name, Host host) {
        this.name = name;
        this.host = host;
        this.sender = Sender.from(host);
        this.worker = new Worker();
    }

    void initialize(TopicQueueRegistry registry, ObjectProvider<DeadLetterQueue> dlqProvider) {
        this.registry = registry;
        this.dlqProvider = dlqProvider;
    }

    void start() {
        worker.start();
    }

    void stop() {
        worker.stop();
    }

    private class Worker {

        final Thread thread;

        Worker() {
            this.thread = new Thread(new Job(), "mmmq-dispatcher-" + name + "-worker");
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
                while (!Thread.currentThread().isInterrupted()) {
                    boolean processed = false;
                    for (TopicQueue topicQueue : registry.getAll()) {
                        Topic topic = topicQueue.topic();
                        long offset = offsets.computeIfAbsent(topic, t -> new AtomicLong(0)).get();
                        Message message = topicQueue.get(offset).orElse(null);
                        if (message != null) {
                            send(message);
                            offsets.get(topic).incrementAndGet();
                            processed = true;
                        }
                    }
                    if (!processed) {
                        try {
                            registry.awaitNewMessage();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }

            void send(Message message) {
                long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (!sender.send(message, MAX_NACK_RETRY_COUNT)) {
                            log.warn("NACK exhausted. Sending to DLQ: {}", message);
                            dlqProvider.stream().forEach(dlq -> dlq.add(new DeadLetter(message)));
                        }
                        return;
                    } catch (Exception e) {
                        log.warn("Communication failure. Backing off {}ms. Error: {}", currentBackoffDelay, e.getMessage());
                        try {
                            Thread.sleep(currentBackoffDelay);
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        currentBackoffDelay = Math.min(currentBackoffDelay * BACKOFF_MULTIPLIER, MAX_BACKOFF_DELAY_MS);
                    }
                }
            }
        }
    }
}
