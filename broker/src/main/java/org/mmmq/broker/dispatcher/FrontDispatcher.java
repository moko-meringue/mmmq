package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import java.util.List;
import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;
    final TopicQueueRegistry registry;
    private final ApplicationEventPublisher publisher;

    public FrontDispatcher(
            List<Dispatcher> dispatchers,
            TopicQueueRegistry registry,
            ApplicationEventPublisher publisher
    ) {
        this.dispatchers = dispatchers;
        this.registry = registry;
        this.publisher = publisher;
    }

    @PreDestroy
    void destroy() {
        dispatchers.forEach(Dispatcher::stop);
    }

    public void dispatch(Message message) {
        boolean anyMatch = dispatchers.stream()
                .anyMatch(dispatcher -> dispatcher.matches(message.topic()));
        if (!anyMatch) {
            return;
        }
        TopicQueue queue = registry.getOrCreateQueue(message.topic());
        queue.add(message);
        publisher.publishEvent(new MessageArrivedEvent(queue));
    }
}
