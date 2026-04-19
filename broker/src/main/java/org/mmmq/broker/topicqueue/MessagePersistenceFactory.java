package org.mmmq.broker.topicqueue;

public interface MessagePersistenceFactory {

    MessagePersistence create(String topicName);
}
