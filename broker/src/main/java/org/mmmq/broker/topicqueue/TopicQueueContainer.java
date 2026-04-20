package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueContainer implements MessageRestorer {

    private final MessagePersistenceFactory persistenceFactory;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueContainer(
            MessagePersistenceFactory persistenceFactory,
            ApplicationEventPublisher applicationEventPublisher
    ) {
        this.persistenceFactory = persistenceFactory;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @EventListener(TopicQueueReadyEvent.class)
    public void onTopicQueueReadyEvent() {
        queues.values().forEach(
                topicQueue -> applicationEventPublisher.publishEvent(new TopicQueueInitializedEvent(topicQueue)));
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, key -> {
            TopicQueue topicQueue = new TopicQueue(key, persistenceFactory.create(key.name()));
            applicationEventPublisher.publishEvent(new TopicQueueInitializedEvent(topicQueue));
            return topicQueue;
        });
    }

    @Override
    public void restore(Message message, int segmentIndex) {
        queues.computeIfAbsent(message.topic(), key -> new TopicQueue(key, persistenceFactory.create(key.name())))
                .restore(message, segmentIndex);
    }
}
