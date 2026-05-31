package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.TopicQueueContainer;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FrontDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FrontDispatcher.class);

    private final TopicQueueContainer container;
    private final DispatcherContainer dispatcherContainer;

    public FrontDispatcher(TopicQueueContainer container, DispatcherContainer dispatcherContainer) {
        this.container = container;
        this.dispatcherContainer = dispatcherContainer;
    }

    public Acknowledgement dispatch(Message message) {
        TopicQueue queue = container.get(message.topic());
        if (!queue.offer(message)) {
            return Acknowledgement.NACK;
        }
        dispatcherContainer.subscribers(queue).forEach(dispatcher -> {
            try {
                dispatcher.drain(queue);
            } catch (Exception exception) {
                log.warn(
                        "Dispatcher '{}' failed during drain on topic '{}'",
                        dispatcher.consumerId(),
                        queue.getTopic(),
                        exception
                );
            }
        });
        return Acknowledgement.ACK;
    }
}
