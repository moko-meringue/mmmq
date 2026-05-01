package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.CheckpointFile;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicQueue implements Closeable { // 한 토픽의 메시지를 디스크에 저장하고 dispatcher에게 제공하는 단일 진실 저장소

    private static final Logger log = LoggerFactory.getLogger(TopicQueue.class);
    private final Topic topic; // 이 큐가 속한 토픽
    private final SegmentFileChain segmentFileChain; // 세그먼트 파일 관리 객체. 실제 디스크 I/O를 담당
    private final CheckpointDirectory checkpointDirectory; // dispatcher 이름별 Checkpoint 컬렉션. checkpoints 디렉토리를 자체 관리
    private final ReentrantLock writeLock = new ReentrantLock(); // offer는 단일 writer만 허용. reader(peek)는 lock 없이 FileChannel positional read

    public TopicQueue(Topic topic, SegmentFileChain segmentFileChain, CheckpointDirectory checkpointDirectory) {
        this.topic = topic;
        this.segmentFileChain = segmentFileChain;
        this.checkpointDirectory = checkpointDirectory;
    }

    public Offset subscribe(String dispatcherName) {
        return new Offset(checkpointDirectory.register(dispatcherName).read());
    }

    public boolean offer(Message message) { // 메시지를 디스크에 기록. fsync 완료 후 true 반환. IOException 발생 시 false 반환 → NACK
        writeLock.lock(); // 동시에 여러 producer가 쓰면 세그먼트 회전 타이밍이 꼬일 수 있어 직렬화
        try {
            segmentFileChain.append(message); // .mmm 쓰기 + fsync, .idx 쓰기 + fsync
            return true; // 두 fsync가 모두 완료된 시점에만 true를 반환 → Producer에 ACK
        } catch (StorageException exception) {
            log.error("Failed to persist message for topic {}", topic, exception);
            return false; // 디스크 쓰기 실패: Producer에 NACK를 보내 재시도 유도
        } finally {
            writeLock.unlock(); // 성공/실패 모두 lock 해제
        }
    }

    @Nullable
    public Message peek(Offset offset) {
        return segmentFileChain.readAt(offset.value()); // lock 없음: FileChannel.read(buf, pos)는 thread-safe
    }

    public Offset commit(String dispatcherName, Offset offset) { // 메시지 처리 완료 후 호출. 진전된 새 Offset을 반환하고 파일에 fsync
        CheckpointFile checkpointFile = checkpointDirectory.get(dispatcherName);
        if (checkpointFile == null) { // subscribe 없이 commit을 호출하면 프로그래밍 오류 — 부재 해석은 토픽 레이어의 책임
            throw new IllegalStateException(
                    "Cannot commit: dispatcher '" + dispatcherName + "' has not subscribed topic '" + topic.name() + "'"
            );
        }
        Offset next = offset.next(); // 다음 peek에서 이 메시지가 아닌 다음 메시지를 읽도록 진전
        checkpointFile.write(next.value()); // fsync #3: 이 시점 이후 브로커 재시작 시 진전된 offset부터 재개
        return next;
    }

    public Topic getTopic() { // FrontDispatcher와 Dispatcher가 토픽 매칭 여부를 확인할 때 사용
        return topic;
    }

    @Override
    public void close() { // 보유한 storage 리소스 정리. SegmentChain 닫기에 실패해도 CheckpointRegistry는 닫힘
        try {
            segmentFileChain.close();
        } finally {
            checkpointDirectory.close();
        }
    }
}
