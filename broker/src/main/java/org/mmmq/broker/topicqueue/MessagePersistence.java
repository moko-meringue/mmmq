package org.mmmq.broker.topicqueue;

import org.mmmq.core.message.Message;

public interface MessagePersistence {

    void persist(Message message, int index);

    void evict(int index);
}
