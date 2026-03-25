package org.mmmq.broker.dispatcher;

import org.springframework.context.ApplicationEvent;

class MessageArrivedEvent extends ApplicationEvent {

    private final TopicQueue topicQueue;

    MessageArrivedEvent(TopicQueue topicQueue) {
        super(topicQueue);
        this.topicQueue = topicQueue;
    }

    TopicQueue getTopicQueue() {
        return topicQueue;
    }
}
