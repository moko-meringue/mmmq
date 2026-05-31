package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.core.PriorityOrdered;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DispatcherContainer implements SmartInitializingSingleton, PriorityOrdered {

    private static final Logger log = LoggerFactory.getLogger(DispatcherContainer.class);

    private final ObjectProvider<Dispatcher> dispatcherProvider;
    private final Map<String, Dispatcher> handlerIdToDispatcher = new HashMap<>();
    private final Map<TopicQueue, List<Dispatcher>> queueToSubscribers = new ConcurrentHashMap<>();

    public DispatcherContainer(ObjectProvider<Dispatcher> dispatcherProvider) {
        this.dispatcherProvider = dispatcherProvider;
    }

    @Override
    public void afterSingletonsInstantiated() {
        dispatcherProvider.stream()
                .forEach(dispatcher -> {
                    Dispatcher previous = handlerIdToDispatcher.putIfAbsent(dispatcher.handlerId(), dispatcher);
                    if (previous != null) {
                        throw new IllegalStateException(
                                "Duplicate handlerId '" + dispatcher.handlerId() + "' across multiple Dispatcher beans"
                        );
                    }
                });
    }

    public void onTopicQueueInitialized(TopicQueue topicQueue) {
        List<Dispatcher> matched = handlerIdToDispatcher.values().stream()
                .filter(dispatcher -> dispatcher.matches(topicQueue.getTopic()))
                .toList();
        matched.forEach(dispatcher -> dispatcher.subscribe(topicQueue));
        queueToSubscribers.put(topicQueue, matched);
    }

    public void dispatch(TopicQueue topicQueue) {
        List<Dispatcher> subscribers = queueToSubscribers.get(topicQueue);
        if (subscribers == null) {
            return;
        }
        subscribers.forEach(dispatcher -> {
            try {
                dispatcher.drain(topicQueue);
            } catch (Exception exception) {
                log.warn(
                        "Dispatcher '{}' failed during drain on topic '{}'",
                        dispatcher.handlerId(),
                        topicQueue.getTopic(),
                        exception
                );
            }
        });
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
