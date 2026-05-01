package org.mmmq.broker.topicqueue;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private static final Logger log = LoggerFactory.getLogger(TopicQueueRegistry.class);

    private final TopicQueueFactory factory;
    private final ApplicationEventPublisher publisher;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueRegistry(TopicQueueFactory factory, ApplicationEventPublisher publisher) {
        this.factory = factory;
        this.publisher = publisher;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, t -> {
            TopicQueue queue = factory.create(t);
            log.info("Topic queue created: {}", queue.getTopic().name());
            publisher.publishEvent(new TopicQueueInitializedEvent(queue));
            return queue;
        });
    }

    void register(TopicQueue queue) {
        queues.put(queue.getTopic(), queue);
        log.info("Topic queue registered: {}", queue.getTopic().name());
        publisher.publishEvent(new TopicQueueInitializedEvent(queue));
    }

    @PreDestroy
    public void destroy() {
        queues.values().forEach(queue -> {
            try {
                queue.close();
            } catch (Exception exception) {
                log.error("Failed to close topic queue: {}", queue.getTopic(), exception);
            }
        });
    }
}
