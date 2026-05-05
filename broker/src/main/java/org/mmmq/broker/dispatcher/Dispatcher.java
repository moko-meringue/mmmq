package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.broker.topicqueue.Offset;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueInitializedEvent;
import org.mmmq.broker.topicqueue.storage.CorruptionException;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

public class Dispatcher {

    private static final int MAX_NACK_RETRY_COUNT = 3;
    private static final long INITIAL_BACKOFF_DELAY_MS = 1000;
    private static final long MAX_BACKOFF_DELAY_MS = 60000;
    private static final int BACKOFF_MULTIPLIER = 2;

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private static final Pattern NAME_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    final String name;
    final Host host;
    final List<TopicPattern> patterns;
    final List<DeadLetterQueue> deadLetterQueues;
    final ConcurrentHashMap<TopicQueue, Offset> subscriptions = new ConcurrentHashMap<>();
    final WorkerPool workerPool = new WorkerPool();
    Sender sender;

    public Dispatcher(String name, Host host, List<TopicPattern> patterns) {
        this(name, host, patterns, List.of());
    }

    public Dispatcher(String name, Host host, List<TopicPattern> patterns, List<DeadLetterQueue> deadLetterQueues) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Dispatcher name must match [A-Za-z0-9._-]+, but was: " + name);
        }
        this.name = name;
        this.host = host;
        this.patterns = patterns;
        this.deadLetterQueues = deadLetterQueues;
        this.sender = Sender.from(host);
    }

    @EventListener(ApplicationReadyEvent.class)
    void onApplicationReady() {
        subscriptions.keySet()
                .forEach(topicQueue -> workerPool.submit(topicQueue, () -> drain(topicQueue)));
    }

    @EventListener
    void onTopicQueueInitialized(TopicQueueInitializedEvent event) {
        TopicQueue topicQueue = event.topicQueue();
        if (!matches(topicQueue.getTopic())) {
            return;
        }
        subscriptions.computeIfAbsent(topicQueue, queue -> queue.subscribe(name));
    }

    @EventListener
    void onMessageArrived(MessageArrivedEvent event) {
        TopicQueue topicQueue = event.topicQueue();
        if (!subscriptions.containsKey(topicQueue)) {
            return;
        }
        workerPool.submit(topicQueue, () -> drain(topicQueue));
    }

    boolean matches(Topic topic) {
        return patterns.stream()
                .anyMatch(pattern -> pattern.matches(topic));
    }

    private void drain(TopicQueue topicQueue) {
        try {
            Offset offset = subscriptions.get(topicQueue);
            while (true) {
                try {
                    Message message = topicQueue.peek(offset);
                    if (message == null) {
                        return;
                    }
                    deliver(message);
                } catch (CorruptionException exception) {
                    log.error("Dispatcher {} skipped corrupted entry on topic {} at offset {}",
                            name,
                            topicQueue.getTopic(),
                            offset,
                            exception
                    );
                }
                offset = topicQueue.commit(name, offset);
                subscriptions.put(topicQueue, offset);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("Dispatcher {} drain interrupted on topic {}", name, topicQueue.getTopic());
        } catch (Exception exception) {
            log.error("Dispatcher {} aborted drain on topic {}", name, topicQueue.getTopic(), exception);
        }
    }

    private void deliver(Message message) throws InterruptedException {
        long currentBackoffDelay = INITIAL_BACKOFF_DELAY_MS;
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                if (!sender.send(message, MAX_NACK_RETRY_COUNT)) {
                    log.warn("NACK exhausted. Sending to DLQ: {}", message);
                    deadLetterQueues.forEach(dlq -> dlq.add(new DeadLetter(message)));
                }
                return;
            } catch (RuntimeException exception) {
                log.warn(
                        "Communication failure. Backing off {}ms. Error: {}",
                        currentBackoffDelay,
                        exception.getMessage()
                );
                Thread.sleep(currentBackoffDelay);
                currentBackoffDelay = Math.min(currentBackoffDelay * BACKOFF_MULTIPLIER, MAX_BACKOFF_DELAY_MS);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        workerPool.shutdownAll();
    }

    private static class WorkerPool {

        private final Map<TopicQueue, ExecutorService> pool = new ConcurrentHashMap<>();

        private void submit(TopicQueue topicQueue, Runnable task) {
            pool.computeIfAbsent(topicQueue, queue -> createWorker()).submit(task);
        }

        private ExecutorService createWorker() {
            return new ThreadPoolExecutor(
                    0, 1, 60L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(1),
                    new ThreadPoolExecutor.DiscardPolicy()
            );
        }

        private void shutdownAll() {
            pool.values()
                    .forEach(ExecutorService::shutdownNow);
            pool.clear();
        }
    }
}
