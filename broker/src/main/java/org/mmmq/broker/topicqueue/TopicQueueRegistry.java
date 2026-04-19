package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final MessagePersistenceFactory persistenceFactory;

    public TopicQueueRegistry(
            ObjectProvider<Dispatcher> dispatcherProvider,
            MessagePersistenceFactory persistenceFactory
    ) {
        this.dispatcherProvider = dispatcherProvider;
        this.persistenceFactory = persistenceFactory;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    private TopicQueue create(Topic topic) {
        MessagePersistence persistence = persistenceFactory.create(topic.name());
        TopicQueue topicQueue = new TopicQueue(topic, persistence);
        for (Dispatcher dispatcher : dispatcherProvider) {
            dispatcher.subscribe(topicQueue);
        }

        return topicQueue;
    }
}
