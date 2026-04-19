package org.mmmq.broker.fixture;

import org.mmmq.broker.topicqueue.MessagePersistence;
import org.mmmq.core.message.Message;

public class NoOpTopicWal implements MessagePersistence {

    @Override
    public void persist(Message message, int index) {
    }

    @Override
    public void evict(int index) {
    }
}
