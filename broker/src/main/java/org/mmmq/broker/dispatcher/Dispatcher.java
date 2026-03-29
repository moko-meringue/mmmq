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
    final ConcurrentHashMap<Topic, Subscription> subscriptions = new ConcurrentHashMap<>();
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

    public boolean matches(Topic topic) {
        return patterns.stream().anyMatch(pattern -> pattern.matches(topic));
    }

    void stop() {
        subscriptions.values().forEach(sub -> sub.executor().shutdownNow());
    }

    void startWorkerFor(TopicQueue topicQueue) {
        if (!matches(topicQueue.getTopic())) {
            return;
        }
        Offset offset = topicQueue.getNewOffset();
        ExecutorService executor = new ThreadPoolExecutor(
                0, 1, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.DiscardPolicy()
        );
        // TODO: 기존 구독이 있는지 확인, 동시성 제어 필요.
        subscriptions.put(topicQueue.getTopic(), new Subscription(topicQueue, offset, executor));
    }

    @EventListener
    void onMessageArrived(MessageArrivedEvent event) {
        Subscription sub = subscriptions.get(event.getTopicQueue().getTopic());
        if (sub == null) {
            return;
        }
        sub.executor().submit(() -> drain(sub.topicQueue(), sub.offset()));
    }

    void drain(TopicQueue topicQueue, Offset offset) {
        while (topicQueue.hasMessageAt(offset)) {
            send(topicQueue.poll(offset));
        }
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
            TopicQueue topicQueue,
            Offset offset,
            ExecutorService executor
    ) {
    }
}
