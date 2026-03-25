package org.mmmq.broker.dispatcher;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

public class Dispatcher {

    static final int MAX_NACK_RETRY_COUNT = 3;
    static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    static final long MAX_BACKOFF_DELAY_MS = 60000;
    static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    private final ConcurrentHashMap<Topic, Worker> workers = new ConcurrentHashMap<>();
    Sender sender;
    private ObjectProvider<DeadLetterQueue> deadLetterQueueProvider;

    public Dispatcher(String name, Host host) {
        this.name = name;
        this.host = host;
        this.sender = Sender.from(host);
    }

    void initialize(TopicQueueRegistry registry, ObjectProvider<DeadLetterQueue> deadLetterQueueProvider) {
        this.deadLetterQueueProvider = deadLetterQueueProvider;
        registry.onNewQueue(this::startWorkerFor);
    }

    void stop() {
        workers.values().forEach(Worker::stop);
    }

    private void startWorkerFor(TopicQueue topicQueue) {
        Worker worker = new Worker(topicQueue);
        workers.put(topicQueue.getTopic(), worker);
        worker.start();
    }

    private void send(Message message) {
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (!sender.send(message, MAX_NACK_RETRY_COUNT)) {
                    log.warn("NACK exhausted. Sending to DLQ: {}", message);
                    deadLetterQueueProvider.stream().forEach(
                            deadLetterQueue -> deadLetterQueue.add(new DeadLetter(message)));
                }
                return;
            } catch (Exception exception) {
                log.warn("Communication failure. Backing off {}ms. Error: {}", currentBackoffDelay,
                        exception.getMessage());
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

    private class Worker {

        final Thread thread;

        Worker(TopicQueue topicQueue) {
            Cursor cursor = topicQueue.subscribe();
            this.thread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        // TopicQueue에 cursor 커서를 전달하여 다음 메시지를 가져옵니다.
                        Message message = topicQueue.take(cursor);
                        send(message);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, "mmmq-dispatcher-" + name + "-worker-" + topicQueue.getTopic().name());
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
