package org.mmmq.broker.topicqueue;

import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.wal.TopicWal;
import org.mmmq.broker.wal.WalDirectory;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final WalDirectory walDirectory;

    public TopicQueueRegistry(
            ObjectProvider<Dispatcher> dispatcherProvider,
            WalDirectory walDirectory
    ) {
        this.dispatcherProvider = dispatcherProvider;
        this.walDirectory = walDirectory;
    }

    public TopicQueue get(Topic topic) {
        return queues.computeIfAbsent(topic, this::create);
    }

    private TopicQueue create(Topic topic) {
        TopicWal topicWal = walDirectory.topicWalFor(topic.name());
        TopicQueue topicQueue = new TopicQueue(topic, topicWal);
        for (Dispatcher dispatcher : (Iterable<Dispatcher>) dispatcherProvider::iterator) {
            dispatcher.subscribe(topicQueue);
        }

        return topicQueue;
    }
}
