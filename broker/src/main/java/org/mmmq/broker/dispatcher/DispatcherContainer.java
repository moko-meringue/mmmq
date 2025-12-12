package org.mmmq.broker.dispatcher;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class DispatcherContainer {

    final List<Dispatcher> dispatchers;

    public DispatcherContainer(
            DeadLetterQueue deadLetterQueue,
            List<DispatcherDefinition> dispatcherDefinitions
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

    @PostConstruct
    void startAll() {
        dispatchers.forEach(Dispatcher::start);
    }

    @PreDestroy
    void stopAll() {
        dispatchers.forEach(Dispatcher::stop);
    }
}
