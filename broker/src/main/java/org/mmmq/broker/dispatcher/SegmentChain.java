package org.mmmq.broker.dispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;

/**
 * Segment 리스트를 '로그(Log)'처럼 관리하며, 관련된 모든 책임을 갖는 클래스입니다. - 절대 오프셋 기반 메시지 조회 - 활성 커서(Cursor) 목록 추적 - 오래된 Segment 메모리 회수
 * (trim)
 * <p>
 * 이 클래스는 자체적인 Lock을 통해 모든 연산의 스레드 안전성을 보장합니다.
 */
class SegmentChain {

    private static final int DEFAULT_SEGMENT_CAPACITY = 1000;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Segment> segments = new ArrayList<>();
    private final int segmentCapacity;
    private final Set<Cursor> cursors = ConcurrentHashMap.newKeySet();

    // trim 로직이 다음 번에 검사를 시작할 위치를 나타냅니다.
    private int headIndex = 0;

    SegmentChain() {
        this(DEFAULT_SEGMENT_CAPACITY);
    }

    SegmentChain(int segmentCapacity) {
        this.segmentCapacity = segmentCapacity;
        segments.add(new Segment(this.segmentCapacity));
    }

    Cursor createAndRegisterCursor() {
        Cursor cursor = new Cursor();
        cursors.add(cursor);
        return cursor;
    }

    void add(Message message) {
        this.lock.lock();
        try {
            Segment tail = getTailSegment();
            if (tail.isFull()) {
                tail = new Segment(this.segmentCapacity);
                segments.add(tail);
            }
            tail.put(message);
        } finally {
            this.lock.unlock();
        }
    }

    boolean hasNext(Cursor cursor) {
        long offset = cursor.getOffset();
        if (offset < 0) {
            throw new IllegalArgumentException("Cursor's offset cannot be negative");
        }
        int segmentIndex = getLogicalSegmentIndex(offset);
        // 리스트 범위를 벗어나거나, 이미 trim 되어 null 처리된 세그먼트인 경우 false 반환
        if (segmentIndex >= segments.size() || segments.get(segmentIndex) == null) {
            throw new IllegalArgumentException("Cursor's offset is out of bounds or has been trimmed: " + offset);
        }
        this.lock.lock();
        try {
            Segment segment = segments.get(segmentIndex);
            int indexInSegment = getIndexInSegment(offset);

            return indexInSegment < segment.getSize();
        } finally {
            this.lock.unlock();
        }
    }

    Message getMessageAt(Cursor cursor) {
        this.lock.lock();
        try {
            long offset = cursor.getOffset();
            Message message = findMessageByOffset(offset);
            updateCursorAndTrimIfNeeded(cursor, offset);

            return message;
        } finally {
            this.lock.unlock();
        }
    }

    private Message findMessageByOffset(long offset) {
        int segmentIndex = getLogicalSegmentIndex(offset);
        int indexInSegment = getIndexInSegment(offset);
        Segment segment = segments.get(segmentIndex);
        return segment.getMessageAt(indexInSegment);
    }

    private void updateCursorAndTrimIfNeeded(Cursor cursor, long oldOffset) {
        cursor.incrementOffset();
        long newOffset = cursor.getOffset();

        if (getLogicalSegmentIndex(oldOffset) != getLogicalSegmentIndex(newOffset)) {
            trim();
        }
    }

    /**
     * 가장 느린 소비자의 offset을 기준으로, 더 이상 필요 없는 오래된 Segment들을 메모리에서 제거(null 처리)합니다.
     */
    private void trim() {
        long minOffset = findMinOffset();
        if (minOffset == 0) {
            return;
        }

        while (headIndex < segments.size()) {
            // 현재 headIndex 세그먼트의 마지막 절대 오프셋 계산
            long headSegmentEndOffset = ((long) (headIndex + 1) * segmentCapacity) - 1;

            // 모든 소비자가 이 세그먼트를 지나쳤다면 null 처리하여 GC 대상이 되게 함
            if (headSegmentEndOffset < minOffset) {
                segments.set(headIndex, null);
                headIndex++;
            } else {
                break;
            }
        }
    }

    private long findMinOffset() {
        return cursors.stream()
                .mapToLong(Cursor::getOffset)
                .min()
                .orElse(0L);
    }

    private int getLogicalSegmentIndex(long offset) {
        return (int) offset / segmentCapacity;
    }

    private int getIndexInSegment(long offset) {
        return (int) (offset % segmentCapacity);
    }

    private Segment getTailSegment() {
        return segments.get(segments.size() - 1);
    }
}
