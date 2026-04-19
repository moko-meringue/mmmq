package org.mmmq.broker.topicqueue;

import org.mmmq.core.message.Message;

public interface MessagePersistence {

    void persist(Message message, int segmentIndex);

    void evict(int segmentIndex);
}
