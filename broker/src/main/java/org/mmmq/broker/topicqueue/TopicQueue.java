package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
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
    // segmentIndex → Segment. 현재 메모리에 올라와 있는 세그먼트만 보관한다.
    private final Map<Integer, Segment> segments = new ConcurrentHashMap<>();
    // 이 TopicQueue를 구독 중인 모든 Dispatcher의 Offset 객체. evict 시점 계산에 사용한다.
    private final Set<Offset> offsets = ConcurrentHashMap.newKeySet();
    // 현재 메모리에 남아있는 가장 오래된 세그먼트의 인덱스.
    private int headSegmentIndex = 0;
    // 현재 새 메시지가 기록 중인 마지막 세그먼트의 인덱스.
    private int tailSegmentIndex = 0;
    // 지금까지 evict된 메시지의 누적 수.
    // Offset.value는 절대 카운터(소비한 메시지 수)이므로,
    // poll()에서 세그먼트 내 상대 위치를 구할 때 이 값을 빼야 한다.
    private long evictedCount = 0;

    public TopicQueue(Topic topic, MessagePersistence messagePersistence) {
        this.topic = topic;
        this.messagePersistence = messagePersistence;
    }

    // 복구용: 현재 메모리에 있는 가장 오래된 메시지부터 소비를 시작하는 Offset을 발급한다.
    // evictedCount = 지금까지 evict된 메시지 수 = 첫 번째 유효 메시지의 절대 인덱스.
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

    // 신규 Dispatcher용: 지금 이 시점 이후에 들어오는 메시지부터 소비하는 Offset을 발급한다.
    // evictedCount + 현재 메모리의 전체 메시지 수 = 다음 메시지가 들어올 절대 위치.
    public Offset getOffsetAtTail() {
        lock.lock();
        try {
            long totalSize = IntStream.rangeClosed(headSegmentIndex, tailSegmentIndex)
                    .mapToObj(segments::get)
                    .filter(Objects::nonNull)
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
    // tail 세그먼트가 꽉 찼으면 새 세그먼트로 넘어가고, WAL에도 기록한다.
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
    // 첫 번째 restore 시 headSegmentIndex를 초기화한다.
    // (이전 실행에서 일부 WAL 파일이 이미 삭제됐다면 headSegmentIndex가 0이 아닐 수 있다.)
    void restore(Message message, int segmentIndex) {
        lock.lock();
        try {
            if (segments.isEmpty()) {
                headSegmentIndex = segmentIndex;
            }
            tailSegmentIndex = Math.max(tailSegmentIndex, segmentIndex);
            segments.computeIfAbsent(segmentIndex, index -> new Segment()).put(message);
        } finally {
            lock.unlock();
        }
    }

    // offset이 가리키는 절대 위치의 메시지를 반환한다.
    // offset.value - evictedCount = head 세그먼트 기준 상대 위치(remaining).
    // headSegmentIndex부터 순서대로 세그먼트를 탐색하며 remaining을 줄여나가다가,
    // remaining < segment.size 인 세그먼트에서 해당 위치의 메시지를 꺼낸다.
    // 예) evictedCount=0, offset=999, Segment 0 size=999 → remaining=999 → Segment 1의 position 0.
    // 아직 채워지지 않은 위치를 가리키면 null을 반환한다 (Dispatcher가 소비를 멈춘다).
    @Nullable
    public Message poll(Offset offset) {
        lock.lock();
        try {
            long remaining = offset.getValue() - evictedCount;
            for (int segmentIndex = headSegmentIndex; segmentIndex <= tailSegmentIndex; segmentIndex++) {
                Segment segment = segments.get(segmentIndex);
                if (segment == null) {
                    return null;
                }
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
    // 가장 느린 구독자(minOffset)가 head 세그먼트의 마지막 메시지까지 소비했을 때만 evict한다.
    // evict 후 evictedCount를 증가시킨다. 구독자 Offset은 건드리지 않는다.
    // tail 세그먼트는 현재 쓰기 중이므로 evict하지 않는다.
    private void cleanupOldSegments() {
        Offset minOffset = offsets.stream()
                .min(Offset::compareTo)
                .orElse(null);
        if (minOffset == null) {
            return;
        }
        while (headSegmentIndex < tailSegmentIndex) {
            Segment headSegment = segments.get(headSegmentIndex);
            if (headSegment == null) {
                return;
            }
            // minOffset의 상대 위치(minOffset.value - evictedCount)가 head 세그먼트 size보다
            // 작으면, 가장 느린 구독자가 아직 head 세그먼트 안에 있으므로 evict하지 않는다.
            if (minOffset.getValue() - evictedCount < headSegment.getSize()) {
                return;
            }
            segments.remove(headSegmentIndex);
            messagePersistence.evict(headSegmentIndex);
            evictedCount += headSegment.getSize();
            headSegmentIndex++;
        }
    }
}