package org.mmmq.broker.dispatcher;

import org.mmmq.broker.topicqueue.TopicQueue;

record MessageArrivedEvent(
        TopicQueue topicQueue
) {
}
