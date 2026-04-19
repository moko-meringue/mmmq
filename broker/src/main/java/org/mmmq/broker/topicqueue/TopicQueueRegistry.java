package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry implements MessageRestorer {

    private final MessagePersistenceFactory persistenceFactory;
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueRegistry(
            MessagePersistenceFactory persistenceFactory,
            ObjectProvider<Dispatcher> dispatcherProvider
    ) {
        this.persistenceFactory = persistenceFactory;
        this.dispatcherProvider = dispatcherProvider;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    @Override
    public void restore(Message message) {
        get(message.topic()).restore(message);
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
