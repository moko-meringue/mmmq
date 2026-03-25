package org.mmmq.broker.dispatcher;

import java.util.Collection;
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
        boolean[] isNew = {false};
        TopicQueue queue = queues.computeIfAbsent(topic, topicKey -> {
            isNew[0] = true;
            return new TopicQueue(topicKey, publisher);
        });
        if (isNew[0]) {
            queue.assignWorkers(dispatcherProvider.stream().toList());
        }
        return queue;
    }

    public Collection<TopicQueue> getAll() {
        return queues.values();
    }
}
