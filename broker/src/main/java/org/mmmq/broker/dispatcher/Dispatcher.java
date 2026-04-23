package org.mmmq.broker.dispatcher;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueInitializedEvent;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;

public class Dispatcher {

    static final int MAX_NACK_RETRY_COUNT = 3;
    static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    static final long MAX_BACKOFF_DELAY_MS = 60000;
    static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    final String name;
    final Host host;
    final List<Pattern> patterns;
    final List<DeadLetterQueue> deadLetterQueues;
    final ConcurrentHashMap<TopicQueue, Subscription> subscriptions = new ConcurrentHashMap<>();
    Sender sender;

    public Dispatcher(String name, Host host, List<Pattern> patterns) {
        this(name, host, patterns, List.of());
    }

    public Dispatcher(String name, Host host, List<Pattern> patterns, List<DeadLetterQueue> deadLetterQueues) {
        this.name = name;
        this.host = host;
        this.patterns = patterns;
        this.deadLetterQueues = deadLetterQueues;
        this.sender = Sender.from(host);
    }

    @EventListener
    void onTopicQueueInitialized(TopicQueueInitializedEvent event) {
        TopicQueue topicQueue = event.topicQueue();
        if (!matches(topicQueue.getTopic())) {
            return;
        }
        subscriptions.computeIfAbsent(topicQueue, topic -> new Subscription(topicQueue));
    }

    @EventListener(DispatchReadyEvent.class)
    void onDispatchReady() {
        subscriptions.forEach((topicQueue, subscription) ->
                subscription.submit(() -> drain(topicQueue, subscription))
        );
    }

    @EventListener
    void onMessageArrived(MessageArrivedEvent event) {
        TopicQueue topicQueue = event.topicQueue();
        subscriptions.computeIfPresent(topicQueue, (topic, subscription) -> {
            subscription.submit(() -> drain(topicQueue, subscription));
            return subscription;
        });
    }

    void drain(TopicQueue topicQueue, Subscription subscription) {
        Offset offset = subscription.offset();
        Message message;
        while ((message = topicQueue.poll(offset)) != null) {
            send(message);
        }
    }

    // TODO: 테스트에서만 사용되므로 지워야 함.
    public void subscribe(TopicQueue topicQueue) {
        if (!matches(topicQueue.getTopic())) {
            return;
        }
        subscriptions.computeIfAbsent(topicQueue, topic -> new Subscription(topicQueue));
    }

    boolean matches(Topic topic) {
        return patterns.stream()
                .anyMatch(pattern -> pattern.matches(topic));
    }

    void stop() {
        subscriptions.values()
                .forEach(Subscription::shutdownNow);
    }

    private void send(Message message) {
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (!sender.send(message, MAX_NACK_RETRY_COUNT)) {
                    log.warn("NACK exhausted. Sending to DLQ: {}", message);
                    deadLetterQueues.forEach(dlq -> dlq.add(new DeadLetter(message)));
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

    record Subscription(
            Offset offset,
            ExecutorService worker
    ) {

        Subscription(TopicQueue topicQueue) {
            this(
                    topicQueue.getOffsetAtTail(),
                    new ThreadPoolExecutor(
                            0, 1, 60L, TimeUnit.SECONDS,
                            new ArrayBlockingQueue<>(1),
                            new ThreadPoolExecutor.DiscardPolicy()
                    )
            );
        }

        void submit(Runnable task) {
            worker.submit(task);
        }

        void shutdownNow() {
            worker.shutdownNow();
        }
    }
}
