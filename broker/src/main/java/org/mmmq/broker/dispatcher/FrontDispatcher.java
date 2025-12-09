package org.mmmq.broker.dispatcher;

import java.util.List;

import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;

    public FrontDispatcher(List<Dispatcher> dispatchers) {
        this.dispatchers = dispatchers;
    }

    public void push(Message message) {
        dispatchers.stream()
                .filter(messageDispatcher -> messageDispatcher.isSubscribing(message.topic()))
                .forEach(messageDispatcher -> messageDispatcher.push(message));
    }
}
