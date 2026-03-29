package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;
    final TopicQueueRegistry registry;

    public FrontDispatcher(List<Dispatcher> dispatchers, TopicQueueRegistry registry) {
        this.dispatchers = dispatchers;
        this.registry = registry;
    }

    @PreDestroy
    void destroy() {
        dispatchers.forEach(Dispatcher::stop);
    }

    public void dispatch(Message message) {
        boolean anyMatch = dispatchers.stream().anyMatch(dispatcher -> dispatcher.matches(message.topic()));
        if (!anyMatch) {
            return;
        }
        TopicQueue queue = registry.getOrCreateQueue(message.topic());
        queue.add(message);
    }
}
