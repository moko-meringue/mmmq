package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;

class SegmentChain {

    private static final int DEFAULT_SEGMENT_CAPACITY = 1000;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Segment> segments = new ConcurrentHashMap<>();
    private final int segmentCapacity;
    private final Set<Offset> offsets = ConcurrentHashMap.newKeySet();
    private int headIndex = 0;
    private int tailIndex = 0;

    SegmentChain() {
        this(DEFAULT_SEGMENT_CAPACITY);
    }

    SegmentChain(int segmentCapacity) {
        this.segmentCapacity = segmentCapacity;
        segments.put(tailIndex, new Segment(this.segmentCapacity));
    }

    Offset getNewOffset() {
        Offset offset = new Offset();
        offsets.add(offset);
        return offset;
    }

    void add(Message message) {
        this.lock.lock();
        try {
            Segment tail = segments.get(tailIndex);
            if (tail == null || tail.isFull()) {
                tailIndex++;
                tail = new Segment(this.segmentCapacity);
                segments.put(tailIndex, tail);
            }
            tail.put(message);
        } finally {
            this.lock.unlock();
        }
    }

    @Nullable
    Message get(Offset offset) {
        this.lock.lock();
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
            this.lock.unlock();
        }
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
