package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import org.mmmq.broker.wal.WalWriter;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

public class TopicQueue {

    private final Topic topic;
    private final SegmentChain segmentChain;

    public TopicQueue(Topic topic, WalWriter walWriter) {
        this.topic = topic;
        this.segmentChain = new SegmentChain(walWriter);
    }

    public void offer(Message message, boolean withWal) {
        segmentChain.offer(message, withWal);
    }

    public Offset getNewOffset() {
        return segmentChain.getNewOffset();
    }

    @Nullable
    public Message poll(Offset offset) {
        return segmentChain.poll(offset);
    }

    public Topic getTopic() {
        return this.topic;
    }
}
