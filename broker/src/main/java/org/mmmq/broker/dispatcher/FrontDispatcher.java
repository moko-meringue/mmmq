package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueRegistry;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final TopicQueueRegistry registry;
    private final ApplicationEventPublisher publisher;

    public FrontDispatcher(TopicQueueRegistry registry, ApplicationEventPublisher publisher) {
        this.registry = registry;
        this.publisher = publisher;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = registry.get(message.topic());
        if (queue.offer(message)) {
            publisher.publishEvent(new MessageArrivedEvent(queue));
            return Acknowledgement.ACK;
        }
        return Acknowledgement.NACK;
    }
}
