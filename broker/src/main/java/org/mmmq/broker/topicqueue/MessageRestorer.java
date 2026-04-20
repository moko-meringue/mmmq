package org.mmmq.broker.topicqueue;

import org.mmmq.core.message.Message;

public interface MessageRestorer {

    void restore(Message message, int segmentIndex);
}
