package org.mmmq.broker.topicqueue;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.DispatcherContainer;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueContainer {

    private static final Logger log = LoggerFactory.getLogger(TopicQueueContainer.class);

    private final TopicQueueFactory factory;
    private final DispatcherContainer dispatcherContainer;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueContainer(TopicQueueFactory factory, DispatcherContainer dispatcherContainer) {
        this.factory = factory;
        this.dispatcherContainer = dispatcherContainer;
    }

    public TopicQueue getOrCreate(Topic topic) {
        return queues.computeIfAbsent(topic, key -> {
            TopicQueue queue = factory.create(key);
            log.info("Topic queue created: {}", key.name());
            dispatcherContainer.register(queue);
            return queue;
        });
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
