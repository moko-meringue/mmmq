package org.mmmq.broker.dispatcher;

import jakarta.annotation.PreDestroy;
import java.util.List;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;
    final TopicQueueContainer registry;
    private final ApplicationEventPublisher publisher;

    public FrontDispatcher(
            List<Dispatcher> dispatchers,
            TopicQueueContainer registry,
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

    public boolean dispatch(Message message) {
        boolean anyMatch = dispatchers.stream()
                .anyMatch(dispatcher -> dispatcher.matches(message.topic()));
        if (!anyMatch) {
            return true;
        }
        TopicQueue queue = registry.get(message.topic());
        boolean offered = queue.offer(message);
        if (offered) {
            publisher.publishEvent(new MessageArrivedEvent(queue));
        }
        return offered;
    }
}
