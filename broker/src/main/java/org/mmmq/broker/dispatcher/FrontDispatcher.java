package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    final TopicQueueContainer container;
    private final ApplicationEventPublisher publisher;

    public FrontDispatcher(TopicQueueContainer container, ApplicationEventPublisher publisher) {
        this.container = container;
        this.publisher = publisher;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = container.get(message.topic());
        if (queue.offer(message)) {
            publisher.publishEvent(new MessageArrivedEvent(queue));
            return Acknowledgement.ACK;
        }
        return Acknowledgement.NACK;
    }
}
