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
    final ObjectProvider<DeadLetterQueue> dlqProvider;

    public FrontDispatcher(List<Dispatcher> dispatchers, TopicQueueRegistry registry, ObjectProvider<DeadLetterQueue> dlqProvider) {
        this.dispatchers = dispatchers;
        this.registry = registry;
        this.dlqProvider = dlqProvider;
    }

    @PostConstruct
    void initialize() {
        dispatchers.forEach(dispatcher -> {
            dispatcher.initialize(registry, dlqProvider);
            dispatcher.start();
        });
    }

    @PreDestroy
    void destroy() {
        dispatchers.forEach(Dispatcher::stop);
    }

    public void dispatch(Message message) {
        registry.add(message.topic(), message);
    }
}
