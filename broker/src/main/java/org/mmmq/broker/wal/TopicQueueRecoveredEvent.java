package org.mmmq.broker.wal;

import org.mmmq.broker.topicqueue.TopicQueue;

public record TopicQueueRecoveredEvent(
        TopicQueue topicQueue
) {
}
