package org.mmmq.broker.broker;

import java.util.List;

import org.mmmq.broker.dispatcher.MessageDispatcher;
import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
public class Broker {

    final List<MessageDispatcher> messageDispatchers;

    public Broker(List<MessageDispatcher> messageDispatchers) {
        this.messageDispatchers = messageDispatchers;
    }

    public void push(Message message) {
        messageDispatchers.stream()
                .filter(messageDispatcher -> messageDispatcher.isSubscribing(message.topic()))
                .forEach(messageDispatcher -> messageDispatcher.push(message));
    }
}
