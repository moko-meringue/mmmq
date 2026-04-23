package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicQueue {

    private static final Logger log = LoggerFactory.getLogger(TopicQueue.class);

    private final Topic topic;
    private final MessagePersistence messagePersistence;
    // offer/poll/restore/cleanupOldSegments 모두 이 lock으로 직렬화한다.
    private final ReentrantLock lock = new ReentrantLock();
    // 이 TopicQueue를 구독 중인 모든 Dispatcher의 Offset 객체. evict 가능 시점 계산에 사용한다.
    private final Set<Offset> offsets = new HashSet<>();
    // segmentIndex → Segment. 현재 메모리에 올라와 있는 세그먼트만 보관한다.
    // TreeMap이므로 firstKey()가 항상 가장 오래된 세그먼트, lastKey()가 tail 세그먼트를 가리킨다.
    private final NavigableMap<Integer, Segment> segments = new TreeMap<>(Comparator.naturalOrder());
    // 지금까지 evict된 메시지의 누적 수.
    // Offset.value는 전체 소비 메시지의 절대 카운터이므로,
    // poll()에서 현재 메모리 기준 상대 위치를 구할 때 이 값을 뺀다.
    private long evictedCount = 0;
    // 현재 새 메시지가 기록 중인 마지막 세그먼트의 인덱스.
    private int tailSegmentIndex = 0;

    public TopicQueue(Topic topic, MessagePersistence messagePersistence) {
        this.topic = topic;
        this.messagePersistence = messagePersistence;
    }

    // 복구용: 현재 메모리에서 가장 오래된 메시지부터 소비하는 Offset을 발급한다.
    // evictedCount가 첫 번째 유효 메시지의 절대 위치이므로, Offset 초깃값으로 사용한다.
    public Offset getOffsetAtHead() {
        lock.lock();
        try {
            Offset offset = new Offset(evictedCount);
            offsets.add(offset);
            return offset;
        } finally {
            lock.unlock();
        }
    }

    // 신규 Dispatcher용: 지금 이 시점 이후 메시지부터 소비하는 Offset을 발급한다.
    // evictedCount + 현재 메모리의 전체 메시지 수 = 아직 도착하지 않은 다음 메시지의 절대 위치.
    public Offset getOffsetAtTail() {
        lock.lock();
        try {
            long totalSize = segments.values().stream()
                    .mapToLong(Segment::getSize)
                    .sum();
            Offset offset = new Offset(evictedCount + totalSize);
            offsets.add(offset);
            return offset;
        } finally {
            lock.unlock();
        }
    }

    // 새 메시지를 큐에 추가한다.
    // tail 세그먼트가 꽉 찼으면 새 세그먼트를 열고, WAL에도 기록한다.
    public boolean offer(Message message) {
        lock.lock();
        try {
            int segmentIndex = nextTailIndex();
            messagePersistence.persist(message, segmentIndex);
            segments.computeIfAbsent(segmentIndex, index -> new Segment()).put(message);
            tailSegmentIndex = segmentIndex;
            return true;
        } catch (Exception exception) {
            log.warn("Failed to offer message to topic queue: {}", topic, exception);
            return false;
        } finally {
            lock.unlock();
        }
    }

    // WAL 복구 시 호출된다. WAL 파일에서 읽은 메시지를 원래 segmentIndex에 그대로 넣는다.
    // TreeMap이 key를 오름차순으로 관리하므로 삽입 순서와 무관하게 세그먼트 순서가 보장된다.
    void restore(Message message, int segmentIndex) {
        lock.lock();
        try {
            tailSegmentIndex = Math.max(tailSegmentIndex, segmentIndex);
            segments.computeIfAbsent(segmentIndex, index -> new Segment()).put(message);
        } finally {
            lock.unlock();
        }
    }

    // offset이 가리키는 절대 위치의 메시지를 반환한다.
    // remaining = offset.value - evictedCount: 현재 메모리 기준 상대 위치.
    // 세그먼트를 오름차순으로 순회하며 각 세그먼트 size만큼 remaining을 차감하다가,
    // remaining < segment.size인 세그먼트에서 remaining번째 메시지를 꺼낸다.
    // 해당 위치가 비어있거나 모든 세그먼트를 소진하면 null을 반환한다 (Dispatcher 대기).
    @Nullable
    public Message poll(Offset offset) {
        lock.lock();
        try {
            long remaining = offset.getValue() - evictedCount;
            for (Map.Entry<Integer, Segment> entry : segments.entrySet()) {
                Segment segment = entry.getValue();
                if (remaining < segment.getSize()) {
                    Message message = segment.get((int) remaining);
                    if (message == null) {
                        return null;
                    }
                    updateOffset(offset);
                    return message;
                }
                remaining -= segment.getSize();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public Topic getTopic() {
        return topic;
    }

    // tail 세그먼트가 꽉 찼으면 다음 인덱스를 반환하고, 아니면 현재 인덱스를 그대로 반환한다.
    private int nextTailIndex() {
        Segment tail = segments.get(tailSegmentIndex);
        if (tail != null && tail.isFull()) {
            return tailSegmentIndex + 1;
        }
        return tailSegmentIndex;
    }

    private void updateOffset(Offset offset) {
        offset.increment();
        cleanupOldSegments();
    }

    // 모든 구독자가 소비를 완료한 세그먼트를 메모리와 WAL에서 제거한다.
    // 가장 느린 구독자(minOffset)의 상대 위치가 가장 오래된 세그먼트를 벗어난 경우에만 evict한다.
    // tail 세그먼트(segments.size == 1일 때)는 현재 쓰기 중이므로 evict하지 않는다.
    private void cleanupOldSegments() {
        Offset minOffset = offsets.stream()
                .min(Offset::compareTo)
                .orElse(null);
        if (minOffset == null) {
            return;
        }
        while (segments.size() > 1) {
            int headKey = segments.firstKey();
            Segment headSegment = segments.get(headKey);
            if (minOffset.getValue() - evictedCount < headSegment.getSize()) {
                return;
            }
            segments.remove(headKey);
            messagePersistence.evict(headKey);
            evictedCount += headSegment.getSize();
        }
    }
}
