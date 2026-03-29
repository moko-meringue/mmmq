package org.mmmq.broker.dispatcher;

record MessageArrivedEvent(
        TopicQueue topicQueue
) {
}
