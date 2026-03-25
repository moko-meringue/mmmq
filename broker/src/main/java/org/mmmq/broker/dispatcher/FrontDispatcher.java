package org.mmmq.broker.dispatcher;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.message.Message;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;
    final TopicQueueRegistry registry;
    final ObjectProvider<DeadLetterQueue> deadLetterQueueProvider;

    public FrontDispatcher(List<Dispatcher> dispatchers, TopicQueueRegistry registry, ObjectProvider<DeadLetterQueue> deadLetterQueueProvider) {
        this.dispatchers = dispatchers;
        this.registry = registry;
        this.deadLetterQueueProvider = deadLetterQueueProvider;
    }

    @PostConstruct
    void initialize() {
        dispatchers.forEach(dispatcher -> dispatcher.initialize(registry, deadLetterQueueProvider));
    }

    @PreDestroy
    void destroy() {
        dispatchers.forEach(Dispatcher::stop);
    }

    public void dispatch(Message message) {
        registry.add(message.topic(), message);
    }
}
