package org.mmmq.broker.topicqueue;

import org.mmmq.core.message.Message;

public class TopicQueueRestorer implements MessageRestorer {

    private final TopicQueue topicQueue;

    public TopicQueueRestorer(TopicQueue topicQueue) {
        this.topicQueue = topicQueue;
    }

    @Override
    public void restore(Message message, int segmentIndex) {
        topicQueue.restore(message, segmentIndex);
    }

    public TopicQueue getTopicQueue() {
        return topicQueue;
    }
}
