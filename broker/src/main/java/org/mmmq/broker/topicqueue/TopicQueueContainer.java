package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueContainer implements MessageRestorer {

    private final MessagePersistenceFactory persistenceFactory;
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueContainer(
            MessagePersistenceFactory persistenceFactory,
            ObjectProvider<Dispatcher> dispatcherProvider
    ) {
        this.persistenceFactory = persistenceFactory;
        this.dispatcherProvider = dispatcherProvider;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::createTopicQueue);
    }

    @Override
    public void restore(Message message) {
        get(message.topic()).restore(message);
    }

    private TopicQueue createTopicQueue(Topic topic) {
        TopicQueue topicQueue = new TopicQueue(topic, persistenceFactory.create(topic.name()));
        dispatcherProvider.forEach(dispatcher -> dispatcher.subscribe(topicQueue));

        return topicQueue;
    }
}
