package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

public class TopicQueue {

    private final Topic topic;
    private final SegmentChain segmentChain;

    public TopicQueue(Topic topic) {
        this.topic = topic;
        this.segmentChain = new SegmentChain();
    }

    public void add(Message message) {
        segmentChain.add(message);
    }

    public Offset getNewOffset() {
        return segmentChain.getNewOffset();
    }

    @Nullable
    public Message poll(Offset offset) {
        return segmentChain.get(offset);
    }

    public Topic getTopic() {
        return this.topic;
    }
}
