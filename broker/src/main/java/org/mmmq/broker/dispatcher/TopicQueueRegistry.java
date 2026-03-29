package org.mmmq.broker.dispatcher;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final ApplicationEventPublisher publisher;

    public TopicQueueRegistry(ObjectProvider<Dispatcher> dispatcherProvider, ApplicationEventPublisher publisher) {
        this.dispatcherProvider = dispatcherProvider;
        this.publisher = publisher;
    }

    public TopicQueue getOrCreateQueue(Topic topic) {
        return queues.computeIfAbsent(topic, topicKey -> {
            TopicQueue topicQueue = new TopicQueue(topicKey, publisher);
            topicQueue.assignWorkers(dispatcherProvider.stream().toList());
            return topicQueue;
        });
    }
}
