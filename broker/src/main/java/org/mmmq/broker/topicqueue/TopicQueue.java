package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.wal.WalAppender;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

public class TopicQueue {

    private static final int DEFAULT_SEGMENT_CAPACITY = 1000;

    private final Topic topic;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Segment> segments = new ConcurrentHashMap<>();
    private final int segmentCapacity;
    private final Set<Offset> offsets = ConcurrentHashMap.newKeySet();
    private final WalAppender walWriter;
    private int headIndex = 0;
    private int tailIndex = 0;

    public TopicQueue(Topic topic, WalAppender walWriter) {
        this.topic = topic;
        this.walWriter = walWriter;
        segmentCapacity = DEFAULT_SEGMENT_CAPACITY;
        segments.put(tailIndex, new Segment(segmentCapacity));
    }

    public Offset getNewOffset() {
        Offset offset = new Offset();
        offsets.add(offset);

        return offset;
    }

    public void offer(Message message) {
        lock.lock();
        try {
            walWriter.write(new WalEntry(tailIndex, message));
            appendToTail(message);
        } finally {
            lock.unlock();
        }
    }

    public void restore(Message message) {
        lock.lock();
        try {
            appendToTail(message);
        } finally {
            lock.unlock();
        }
    }

    @Nullable
    public Message poll(Offset offset) {
        lock.lock();
        try {
            int index = offset.getUnitIndex(segmentCapacity);
            Segment segment = segments.get(index);
            if (segment == null) {
                return null;
            }
            int relativeOffset = offset.getRelativeIndex(segmentCapacity);
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

    private void appendToTail(Message message) {
        Segment tail = segments.get(tailIndex);
        if (tail == null || tail.isFull()) {
            tailIndex++;
            tail = new Segment(segmentCapacity);
            segments.put(tailIndex, tail);
        }
        tail.put(message);
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
        int limit = minOffset.getUnitIndex(segmentCapacity);
        if (headIndex < limit) {
            for (int i = headIndex; i < limit; i++) {
                segments.remove(i);
                walWriter.deleteSegmentFile(i);
            }
            headIndex = limit;
        }
    }

    private Segment getSegment(Offset offset) {
        int index = offset.getUnitIndex(segmentCapacity);
        Segment segment = segments.get(index);
        if (segment == null) {
            throw new IllegalArgumentException("Invalid offset: " + offset);
        }

        return segment;
    }
}
