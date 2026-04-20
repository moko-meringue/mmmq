package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueContainer implements MessageRestorer {

    private final MessagePersistenceFactory persistenceFactory;
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();

    public TopicQueueContainer(
            MessagePersistenceFactory persistenceFactory,
            ObjectProvider<Dispatcher> dispatcherProvider,
            ApplicationEventPublisher applicationEventPublisher
    ) {
        this.persistenceFactory = persistenceFactory;
        this.dispatcherProvider = dispatcherProvider;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::createTopicQueue);
    }

    @Override
    public void restore(Message message, int segmentIndex) {
        queues.computeIfAbsent(message.topic(), this::createQueueOnly)
                .restore(message, segmentIndex);
    }

    @EventListener(RestoreCompletedEvent.class)
    void onRestoreCompleted() {
        queues.values().forEach(
                topicQueue -> applicationEventPublisher.publishEvent(new TopicQueueInitializedEvent(topicQueue)));
        applicationEventPublisher.publishEvent(new DispatchReadyEvent());
    }

    private TopicQueue createTopicQueue(Topic topic) {
        TopicQueue topicQueue = new TopicQueue(topic, persistenceFactory.create(topic.name()));
        dispatcherProvider.forEach(dispatcher -> dispatcher.subscribe(topicQueue));
        return topicQueue;
    }

    private TopicQueue createQueueOnly(Topic topic) {
        return new TopicQueue(topic, persistenceFactory.create(topic.name()));
    }
}
