package org.mmmq.broker.dispatcher;

import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.DeadLetterQueue;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

@Component
public class FrontDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FrontDispatcher.class);
    final List<Dispatcher> dispatchers;
    final ObjectProvider<DeadLetterQueue> deadLetterQueueProvider;

    public FrontDispatcher(List<Dispatcher> dispatchers, ObjectProvider<DeadLetterQueue> deadLetterQueueProvider) {
        this.dispatchers = dispatchers;
        this.deadLetterQueueProvider = deadLetterQueueProvider;
    }

    public void dispatch(Message message) {
        dispatchers.stream()
                .filter(dispatcher -> dispatcher.isSubscribing(message.topic()))
                .forEach(messageDispatcher -> push(messageDispatcher, message));
    }

    private void push(Dispatcher dispatcher, Message message) {
        Consumer<Throwable> onFailure = cause -> {
            log.warn("Dispatching failed for message: {}", message, cause);
            DeadLetter deadLetter = new DeadLetter(message, cause);
            deadLetterQueueProvider.stream()
                    .forEach(deadLetterQueue -> deadLetterQueue.add(deadLetter));
        };

        dispatcher.dispatch(new MessageEnvelope(message, onFailure));
    }
}
