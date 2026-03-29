package org.mmmq.broker.dispatcher;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;

class SegmentChain {

    private static final int DEFAULT_SEGMENT_CAPACITY = 1000;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Segment> segments = new ArrayList<>();
    private final int segmentCapacity;
    private final Set<Offset> offsets = ConcurrentHashMap.newKeySet();

    private int headIndex = 0;

    SegmentChain() {
        this(DEFAULT_SEGMENT_CAPACITY);
    }

    SegmentChain(int segmentCapacity) {
        this.segmentCapacity = segmentCapacity;
        segments.add(new Segment(this.segmentCapacity));
    }

    Offset getNewOffset() {
        Offset offset = new Offset();
        offsets.add(offset);
        return offset;
    }

    void add(Message message) {
        this.lock.lock();
        try {
            Segment tail = segments.get(segments.size() - 1);
            if (tail == null || tail.isFull()) {
                tail = new Segment(this.segmentCapacity);
                segments.add(tail);
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
            Segment segment = getSegment(offset);
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
            Collections.fill(segments.subList(headIndex, limit), null);
            headIndex = limit;
        }
    }

    private Segment getSegment(Offset offset) {
        int index = offset.getUnitIndex(this.segmentCapacity);
        if (index >= segments.size() || segments.get(index) == null) {
            throw new IllegalArgumentException("Offset's offset is out of bounds or has been trimmed: " + offset);
        }
        return segments.get(index);
    }
}
