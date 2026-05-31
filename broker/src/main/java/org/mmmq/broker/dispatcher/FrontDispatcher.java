package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    private final TopicQueueContainer container;
    private final DispatcherContainer dispatcherContainer;

    public FrontDispatcher(TopicQueueContainer container, DispatcherContainer dispatcherContainer) {
        this.container = container;
        this.dispatcherContainer = dispatcherContainer;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = container.get(message.topic());
        if (queue.offer(message)) {
            dispatcherContainer.dispatch(queue);
            return Acknowledgement.ACK;
        }
        return Acknowledgement.NACK;
    }
}
