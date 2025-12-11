package org.mmmq.broker.dispatcher;

import java.util.List;
import java.util.stream.Collectors;

import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class DispatcherContainer {

    final List<Dispatcher> dispatchers;

    public DispatcherContainer(
        List<DispatcherDefinition> dispatcherDefinitions,
        DeadLetterQueue deadLetterQueue
    ) {
        this.dispatchers = dispatcherDefinitions.stream()
            .map(dispatcherDefinition -> dispatcherDefinition.toDispatcher(deadLetterQueue))
            .collect(Collectors.toList());
    }

    public List<Dispatcher> getDispatchers(Topic topic) {
        return dispatchers.stream()
                .filter(dispatcher -> dispatcher.isSubscribing(topic))
                .toList();
    }

    void add(Dispatcher dispatcher) {
        dispatchers.add(dispatcher);
    }

    @PostConstruct
    void startAll() {
        dispatchers.forEach(Dispatcher::start);
    }

    @PreDestroy
    void stopAll() {
        dispatchers.forEach(Dispatcher::stop);
    }
}
