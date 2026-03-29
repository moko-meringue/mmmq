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

    public boolean matches(Topic topic) {
        return patterns.stream()
                .anyMatch(pattern -> pattern.matches(topic));
    }

    void stop() {
        subscriptions.values()
                .forEach(Subscription::shutdownNow);
    }

    void subscribe(TopicQueue topicQueue) {
        if (!matches(topicQueue.getTopic())) {
            return;
        }
        subscriptions.computeIfAbsent(topicQueue, topic -> new Subscription(topicQueue));
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
        // TODO: hasMessageAt, poll 메서드는 각각 lock을 사용하는 비효율적인 구조이다.
        // topicQueue에서 남은 메시지를 전부 가져오는 메서드를 만들어서 send를 한 번에 처리하는 구조로 개선할 수 있다.
        // 그러나, 이렇게 구현한다면 특정 Dispatcher가 모든 메시지를 독점적으로 가져가는 상황이 발생할 수 있다.
        // 이를 방지하기 위해, 일정량의 메시지만 가져오는 구조로 개선할 수 있다.
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
            Offset offset,
            ExecutorService worker
    ) {

        Subscription(TopicQueue topicQueue) {
            this(
                    topicQueue.getNewOffset(),
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
