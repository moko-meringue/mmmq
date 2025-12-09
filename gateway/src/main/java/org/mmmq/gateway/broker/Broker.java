package org.mmmq.gateway.broker;

import org.mmmq.core.message.Message;
import org.mmmq.gateway.dispatcher.MessageDispatcher;
import org.springframework.stereotype.Component;

import java.util.List;

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
