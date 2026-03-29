package org.mmmq.broker.dispatcher;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;

    public TopicQueueRegistry(ObjectProvider<Dispatcher> dispatcherProvider) {
        this.dispatcherProvider = dispatcherProvider;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    private TopicQueue create(Topic topic) {
        TopicQueue topicQueue = new TopicQueue(topic);
        dispatcherProvider.stream()
                .forEach(dispatcher -> dispatcher.subscribe(topicQueue));
        return topicQueue;
    }
}
