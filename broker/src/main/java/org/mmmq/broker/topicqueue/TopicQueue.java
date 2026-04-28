package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.topicqueue.storage.OffsetStore;
import org.mmmq.broker.topicqueue.storage.SegmentChain;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicQueue { // 한 토픽의 메시지를 디스크에 저장하고 dispatcher에게 제공하는 단일 진실 저장소

    private static final Logger log = LoggerFactory.getLogger(TopicQueue.class);
    private final Topic topic; // 이 큐가 속한 토픽
    private final SegmentChain segmentChain; // 세그먼트 파일 관리 객체. 실제 디스크 I/O를 담당
    private final Map<String, OffsetStore> offsetStores = new ConcurrentHashMap<>(); // dispatcher 이름 → OffsetStore. 여러 dispatcher가 동시에 독립적으로 offset을 관리
    private final ReentrantLock writeLock = new ReentrantLock(); // offer는 단일 writer만 허용. reader(peek)는 lock 없이 FileChannel positional read

    public TopicQueue(Topic topic, SegmentChain segmentChain) {
        this.topic = topic;
        this.segmentChain = segmentChain;
    }

    public boolean offer(Message message) { // 메시지를 디스크에 기록. fsync 완료 후 true 반환. IOException 발생 시 false 반환 → NACK
        writeLock.lock(); // 동시에 여러 producer가 쓰면 세그먼트 회전 타이밍이 꼬일 수 있어 직렬화
        try {
            segmentChain.append(message); // .mmm 쓰기 + fsync, .idx 쓰기 + fsync

            return true; // 두 fsync가 모두 완료된 시점에만 true를 반환 → Producer에 ACK
        } catch (StorageException exception) {
            log.error("Failed to persist message for topic {}", topic, exception);

            return false; // 디스크 쓰기 실패: Producer에 NACK를 보내 재시도 유도
        } finally {
            writeLock.unlock(); // 성공/실패 모두 lock 해제
        }
    }

    public Offset subscribe(
            String dispatcherName) { // dispatcher 최초 등록 시 호출. OffsetStore를 열고 마지막 커밋 위치에서 시작하는 Offset을 반환
        OffsetStore store = offsetStores.computeIfAbsent(
                dispatcherName,
                name -> OffsetStore.open(segmentChain.offsetsDir(), name) // 파일 없으면 생성(초기값 0), 있으면 기존 값 유지
        );

        return new Offset(store.read()); // 저장된 마지막 commit 위치에서 재개. 브로커 재시작 시 중단 지점부터 다시 시작
    }

    @Nullable
    public Message peek(
            Offset offset) { // offset 위치의 메시지를 읽어 반환하되 offset을 증가시키지 않음. at-least-once의 핵심: commit 전에 브로커가 죽으면 재시작 후 같은 메시지를 다시 읽음
        return segmentChain.readAt(offset.value()); // lock 없음: FileChannel.read(buf, pos)는 thread-safe
    }

    public void commit(String dispatcherName, Offset offset) { // 메시지 처리 완료 후 호출. offset을 1 증가시키고 파일에 fsync
        offset.increment(); // 다음 peek에서 이 메시지가 아닌 다음 메시지를 읽도록 전진
        OffsetStore store = offsetStores.get(dispatcherName);
        if (store == null) { // subscribe 없이 commit을 호출하면 프로그래밍 오류
            throw new IllegalStateException("Dispatcher not subscribed: " + dispatcherName);
        }
        store.write(offset.value()); // fsync #3: 이 시점 이후 브로커 재시작 시 증가된 offset부터 재개
    }

    public Topic getTopic() { // FrontDispatcher와 Dispatcher가 토픽 매칭 여부를 확인할 때 사용
        return topic;
    }
}
