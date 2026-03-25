package org.mmmq.broker.dispatcher;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueRegistry {

    private final ConcurrentHashMap<Topic, TopicQueue> queues = new ConcurrentHashMap<>();
    private final List<Consumer<TopicQueue>> listeners = new CopyOnWriteArrayList<>();

    public void onNewQueue(Consumer<TopicQueue> listener) {
        listeners.add(listener);
    }

    public void add(Topic topic, Message message) {
        boolean[] isNew = {false};
        TopicQueue queue = queues.computeIfAbsent(topic, topicKey -> {
            isNew[0] = true;
            return new TopicQueue(topicKey);
        });
        queue.add(message);
        if (isNew[0]) {
            listeners.forEach(listener -> listener.accept(queue));
        }
    }

    public Collection<TopicQueue> getAll() {
        return queues.values();
    }
}
