package org.mmmq.broker.dispatcher;

import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FrontDispatcher {

    final List<Dispatcher> dispatchers;

    public FrontDispatcher(List<Dispatcher> dispatchers) {
        this.dispatchers = dispatchers;
    }

    public void push(Message message) {
        dispatchers.stream()
                .filter(dispatcher -> dispatcher.isSubscribing(message.topic()))
                .forEach(messageDispatcher -> messageDispatcher.dispatch(message));
    }
}
