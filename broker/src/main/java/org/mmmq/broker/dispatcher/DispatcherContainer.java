package org.mmmq.broker.dispatcher;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.core.identifier.ConsumerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DispatcherContainer {

    private static final Logger log = LoggerFactory.getLogger(DispatcherContainer.class);

    private final List<Dispatcher> dispatchers;
    private final Map<TopicQueue, List<Dispatcher>> subscriptions = new ConcurrentHashMap<>();

    public DispatcherContainer(Collection<Dispatcher> dispatchers) {
        Set<ConsumerId> seen = new HashSet<>();
        dispatchers.forEach(dispatcher -> {
            if (!seen.add(dispatcher.consumerId())) {
                throw new IllegalStateException(
                        "Duplicate consumerId '" + dispatcher.consumerId() + "' across multiple Dispatcher beans"
                );
            }
        });
        this.dispatchers = List.copyOf(dispatchers);
    }

    public void onTopicQueueInitialized(TopicQueue topicQueue) {
        List<Dispatcher> matched = dispatchers.stream()
                .filter(dispatcher -> dispatcher.matches(topicQueue.getTopic()))
                .toList();
        matched.forEach(dispatcher -> dispatcher.subscribe(topicQueue));
        subscriptions.put(topicQueue, matched);
    }

    public void dispatch(TopicQueue topicQueue) {
        List<Dispatcher> subscribers = subscriptions.get(topicQueue);
        if (subscribers == null) {
            return;
        }
        subscribers.forEach(dispatcher -> {
            try {
                dispatcher.drain(topicQueue);
            } catch (Exception exception) {
                log.warn(
                        "Dispatcher '{}' failed during drain on topic '{}'",
                        dispatcher.consumerId(),
                        topicQueue.getTopic(),
                        exception
                );
            }
        });
    }
}
