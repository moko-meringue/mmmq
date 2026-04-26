package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

public class TopicQueue {

    private static final int DEFAULT_SEGMENT_CAPACITY = 1000;

    private final Topic topic;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Segment> segments = new ConcurrentHashMap<>();
    private final int segmentCapacity;
    private final Set<Offset> offsets = ConcurrentHashMap.newKeySet();
    private int headIndex = 0;
    private int tailIndex = 0;

    public TopicQueue(Topic topic) {
        this(topic, DEFAULT_SEGMENT_CAPACITY);
    }

    public TopicQueue(Topic topic, int segmentCapacity) {
        this.topic = topic;
        this.segmentCapacity = segmentCapacity;
        segments.put(tailIndex, new Segment(this.segmentCapacity));
    }

    public void offer(Message message) {
        lock.lock();
        try {
            Segment tail = segments.get(tailIndex);
            if (tail == null || tail.isFull()) {
                tailIndex++;
                tail = new Segment(this.segmentCapacity);
                segments.put(tailIndex, tail);
            }
            tail.put(message);
        } finally {
            lock.unlock();
        }
    }

    public Offset getNewOffset() {
        Offset offset = new Offset();
        offsets.add(offset);

        return offset;
    }

    @Nullable
    public Message poll(Offset offset) {
        lock.lock();
        try {
            int index = offset.getUnitIndex(this.segmentCapacity);
            Segment segment = segments.get(index);
            if (segment == null) {
                return null;
            }

            int relativeOffset = offset.getRelativeIndex(this.segmentCapacity);
            if (!segment.existsAt(relativeOffset)) {
                return null;
            }

            Message message = segment.get(relativeOffset);
            updateOffset(offset);

            return message;
        } finally {
            lock.unlock();
        }
    }

    public Topic getTopic() {
        return topic;
    }

    private void updateOffset(Offset offset) {
        Segment oldSegment = getSegment(offset);
        offset.increment();
        Segment newSegment = getSegment(offset);
        if (oldSegment != newSegment) {
            cleanupOldSegments();
        }
    }

    private void cleanupOldSegments() {
        Offset minOffset = offsets.stream()
                .min(Offset::compareTo)
                .orElse(null);
        if (minOffset == null) {
            return;
        }
        int limit = minOffset.getUnitIndex(this.segmentCapacity);
        if (headIndex < limit) {
            for (int i = headIndex; i < limit; i++) {
                segments.remove(i);
            }
            headIndex = limit;
        }
    }

    private Segment getSegment(Offset offset) {
        int index = offset.getUnitIndex(this.segmentCapacity);
        Segment segment = segments.get(index);
        if (segment == null) {
            throw new IllegalArgumentException("Invalid offset: " + offset);
        }

        return segment;
    }
}